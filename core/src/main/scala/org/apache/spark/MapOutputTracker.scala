/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark

import java.io._
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, ThreadPoolExecutor}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map}
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.util.control.NonFatal
import org.apache.spark.broadcast.{Broadcast, BroadcastManager}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv}
import org.apache.spark.scheduler.{ExecutorCacheTaskLocation, MapStatus}
import org.apache.spark.shuffle.MetadataFetchFailedException
import org.apache.spark.storage.{BlockId, BlockManagerId, ContinuousShuffleBlockId, ShuffleBlockId}
import org.apache.spark.util._

/**
  * Helper class used by the [[MapOutputTrackerMaster]] to perform bookkeeping for a single
  * ShuffleMapStage.
  *
  * This class maintains a mapping from mapIds to `MapStatus`. It also maintains a cache of
  * serialized map statuses in order to speed up tasks' requests for map output statuses.
  *
  * All public methods of this class are thread-safe.
  */
private class ShuffleStatus(numPartitions: Int) {

  // All accesses to the following state must be guarded with `this.synchronized`.

  /**
    * MapStatus for each partition. The index of the array is the map partition id.
    * Each value in the array is the MapStatus for a partition, or null if the partition
    * is not available. Even though in theory a task may run multiple times (due to speculation,
    * stage retries, etc.), in practice the likelihood of a map output being available at multiple
    * locations is so small that we choose to ignore that case and store only a single location
    * for each output.
    */
  // Exposed for testing
  val mapStatuses = new Array[MapStatus](numPartitions)

  /**
    * The cached result of serializing the map statuses array. This cache is lazily populated when
    * [[serializedMapStatus]] is called. The cache is invalidated when map outputs are removed.
    */
  private[this] var cachedSerializedMapStatus: Array[Byte] = _

  /**
    * Broadcast variable holding serialized map output statuses array. When [[serializedMapStatus]]
    * serializes the map statuses array it may detect that the result is too large to send in a
    * single RPC, in which case it places the serialized array into a broadcast variable and then
    * sends a serialized broadcast variable instead. This variable holds a reference to that
    * broadcast variable in order to keep it from being garbage collected and to allow for it to be
    * explicitly destroyed later on when the ShuffleMapStage is garbage-collected.
    */
  private[this] var cachedSerializedBroadcast: Broadcast[Array[Byte]] = _

  /**
    * Counter tracking the number of partitions that have output. This is a performance optimization
    * to avoid having to count the number of non-null entries in the `mapStatuses` array and should
    * be equivalent to`mapStatuses.count(_ ne null)`.
    */
  private[this] var _numAvailableOutputs: Int = 0

  /**
    * Register a map output. If there is already a registered location for the map output then it
    * will be replaced by the new location.
    */
  def addMapOutput(mapId: Int, status: MapStatus): Unit = synchronized {
    if (mapStatuses(mapId) == null) {
      _numAvailableOutputs += 1
      invalidateSerializedMapOutputStatusCache()
    }
    mapStatuses(mapId) = status
  }

  /**
    * Remove the map output which was served by the specified block manager.
    * This is a no-op if there is no registered map output or if the registered output is from a
    * different block manager.
    */
  def removeMapOutput(mapId: Int, bmAddress: BlockManagerId): Unit = synchronized {
    if (mapStatuses(mapId) != null && mapStatuses(mapId).location == bmAddress) {
      _numAvailableOutputs -= 1
      mapStatuses(mapId) = null
      invalidateSerializedMapOutputStatusCache()
    }
  }

  /**
    * Removes all shuffle outputs associated with this host. Note that this will also remove
    * outputs which are served by an external shuffle server (if one exists).
    */
  def removeOutputsOnHost(host: String): Unit = {
    removeOutputsByFilter(x => x.host == host)
  }

  /**
    * Removes all map outputs associated with the specified executor. Note that this will also
    * remove outputs which are served by an external shuffle server (if one exists), as they are
    * still registered with that execId.
    */
  def removeOutputsOnExecutor(execId: String): Unit = synchronized {
    removeOutputsByFilter(x => x.executorId == execId)
  }

  /**
    * Removes all shuffle outputs which satisfies the filter. Note that this will also
    * remove outputs which are served by an external shuffle server (if one exists).
    */
  def removeOutputsByFilter(f: (BlockManagerId) => Boolean): Unit = synchronized {
    for (mapId <- 0 until mapStatuses.length) {
      if (mapStatuses(mapId) != null && f(mapStatuses(mapId).location)) {
        _numAvailableOutputs -= 1
        mapStatuses(mapId) = null
        invalidateSerializedMapOutputStatusCache()
      }
    }
  }

  /**
    * Number of partitions that have shuffle outputs.
    */
  def numAvailableOutputs: Int = synchronized {
    _numAvailableOutputs
  }

  /**
    * Returns the sequence of partition ids that are missing (i.e. needs to be computed).
    */
  def findMissingPartitions(): Seq[Int] = synchronized {
    val missing = (0 until numPartitions).filter(id => mapStatuses(id) == null)
    assert(missing.size == numPartitions - _numAvailableOutputs,
      s"${missing.size} missing, expected ${numPartitions - _numAvailableOutputs}")
    missing
  }

  /**
    * Serializes the mapStatuses array into an efficient compressed format. See the comments on
    * `MapOutputTracker.serializeMapStatuses()` for more details on the serialization format.
    *
    * This method is designed to be called multiple times and implements caching in order to speed
    * up subsequent requests. If the cache is empty and multiple threads concurrently attempt to
    * serialize the map statuses then serialization will only be performed in a single thread and all
    * other threads will block until the cache is populated.
    */
  def serializedMapStatus(broadcastManager: BroadcastManager, isLocal: Boolean, minBroadcastSize: Int): Array[Byte] = synchronized {
    if (cachedSerializedMapStatus eq null) {
      val serResult = MapOutputTracker.serializeMapStatuses(
        mapStatuses, broadcastManager, isLocal, minBroadcastSize)
      cachedSerializedMapStatus = serResult._1
      cachedSerializedBroadcast = serResult._2
    }
    cachedSerializedMapStatus
  }

  // Used in testing.
  def hasCachedSerializedBroadcast: Boolean = synchronized {
    cachedSerializedBroadcast != null
  }

  /**
    * Helper function which provides thread-safe access to the mapStatuses array.
    * The function should NOT mutate the array.
    */
  def withMapStatuses[T](f: Array[MapStatus] => T): T = synchronized {
    f(mapStatuses)
  }

  /**
    * Clears the cached serialized map output statuses.
    */
  def invalidateSerializedMapOutputStatusCache(): Unit = synchronized {
    if (cachedSerializedBroadcast != null) {
      // Prevent errors during broadcast cleanup from crashing the DAGScheduler (see SPARK-21444)
      Utils.tryLogNonFatalError {
        // Use `blocking = false` so that this operation doesn't hang while trying to send cleanup
        // RPCs to dead executors.
        cachedSerializedBroadcast.destroy(blocking = false)
      }
      cachedSerializedBroadcast = null
    }
    cachedSerializedMapStatus = null
  }
}

private[spark] sealed trait MapOutputTrackerMessage

private[spark] case class GetMapOutputStatuses(shuffleId: Int)
  extends MapOutputTrackerMessage

private[spark] case object StopMapOutputTracker extends MapOutputTrackerMessage

private[spark] case class GetMapOutputMessage(shuffleId: Int, context: RpcCallContext)

/** RpcEndpoint class for MapOutputTrackerMaster */
private[spark] class MapOutputTrackerMasterEndpoint(override val rpcEnv: RpcEnv, tracker: MapOutputTrackerMaster, conf: SparkConf)
  extends RpcEndpoint with Logging {

  logDebug("init") // force eager creation of logger

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case GetMapOutputStatuses(shuffleId: Int) =>
      val hostPort = context.senderAddress.hostPort
      logInfo("Asked to send map output locations for shuffle " + shuffleId + " to " + hostPort)
      val mapOutputStatuses = tracker.post(new GetMapOutputMessage(shuffleId, context))

    case StopMapOutputTracker =>
      logInfo("MapOutputTrackerMasterEndpoint stopped!")
      context.reply(true)
      stop()
  }
}

/**
  * Class that keeps track of the location of the map output of a stage. This is abstract because the
  * driver and executor have different versions of the MapOutputTracker. In principle the driver-
  * and executor-side classes don't need to share a common base class; the current shared base class
  * is maintained primarily for backwards-compatibility in order to avoid having to update existing
  * test code.
  */
private[spark] abstract class MapOutputTracker(conf: SparkConf) extends Logging {
  /** Set to the MapOutputTrackerMasterEndpoint living on the driver. */
  var trackerEndpoint: RpcEndpointRef = _

  /**
    * The driver-side counter is incremented every time that a map output is lost. This value is sent
    * to executors as part of tasks, where executors compare the new epoch number to the highest
    * epoch number that they received in the past. If the new epoch number is higher then executors
    * will clear their local caches of map output statuses and will re-fetch (possibly updated)
    * statuses from the driver.
    */
  protected var epoch: Long = 0
  protected val epochLock = new AnyRef

  /**
    * Send a message to the trackerEndpoint and get its result within a default timeout, or
    * throw a SparkException if this fails.
    */
  protected def askTracker[T: ClassTag](message: Any): T = {
    try {
      trackerEndpoint.askSync[T](message)
    } catch {
      case e: Exception =>
        logError("Error communicating with MapOutputTracker", e)
        throw new SparkException("Error communicating with MapOutputTracker", e)
    }
  }

  /** Send a one-way message to the trackerEndpoint, to which we expect it to reply with true. */
  protected def sendTracker(message: Any) {
    val response = askTracker[Boolean](message)
    if (response != true) {
      throw new SparkException(
        "Error reply received from MapOutputTracker. Expecting true, got " + response.toString)
    }
  }

  protected def supportsContinuousBlockBatchFetch(serializerRelocatable: Boolean): Boolean = {
    if (!serializerRelocatable) {
      false
    } else {
      if (!conf.getBoolean("spark.shuffle.compress", true)) {
        true
      } else {
        val compressionCodec: CompressionCodec = CompressionCodec.createCodec(conf)
        CompressionCodec.supportsConcatenationOfSerializedStreams(compressionCodec)
      }
    }
  }

  // For testing
  def getMapSizesByExecutorId(shuffleId: Int, reduceId: Int)
  : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    getMapSizesByExecutorId(shuffleId, reduceId, reduceId + 1, serializerRelocatable = false)
  }

  def getUpdatedStatus(shuffleId: Int, startPartition: Int,
                       endPartition: Int,
                       startMapId: Int,
                       endMapId: Int): Seq[(BlockManagerId, Seq[(BlockId, Long)])]

  /**
    * Called from executors to get the server URIs and output sizes for each shuffle block that
    * needs to be read from a given range of map output partitions (startPartition is included but
    * endPartition is excluded from the range).
    *
    * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
    *         and the second item is a sequence of (shuffle block id, shuffle block size) tuples
    *         describing the shuffle blocks that are stored at that block manager.
    */
  def getMapSizesByExecutorId(
                               shuffleId: Int,
                               startPartition: Int,
                               endPartition: Int,
                               serializerRelocatable: Boolean): Seq[(BlockManagerId, Seq[(BlockId, Long)])]

  /**
    * Called from executors to get the server URIs and output sizes for each shuffle block that
    * needs to be read from a given range of map output partitions (startPartition is included but
    * endPartition is excluded from the range) and a given start map Id and end map Id.
    *
    * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
    *         and the second item is a sequence of (shuffle block id, shuffle block size) tuples
    *         describing the shuffle blocks that are stored at that block manager.
    */
  def getMapSizesByExecutorId(
                               shuffleId: Int,
                               startPartition: Int,
                               endPartition: Int,
                               startMapId: Int,
                               endMapId: Int,
                               serializerRelocatable: Boolean): Seq[(BlockManagerId, Seq[(BlockId, Long)])]

  /**
    * Deletes map output status information for the specified shuffle stage.
    */
  def unregisterShuffle(shuffleId: Int): Unit

  def stop() {}
}

/**
  * Driver-side class that keeps track of the location of the map output of a stage.
  *
  * The DAGScheduler uses this class to (de)register map output statuses and to look up statistics
  * for performing locality-aware reduce task scheduling.
  *
  * ShuffleMapStage uses this class for tracking available / missing outputs in order to determine
  * which tasks need to be run.
  */
private[spark] class MapOutputTrackerMaster(
                                             conf: SparkConf,
                                             broadcastManager: BroadcastManager,
                                             isLocal: Boolean)
  extends MapOutputTracker(conf) {

  // The size at which we use Broadcast to send the map output statuses to the executors
  private val minSizeForBroadcast =
    conf.getSizeAsBytes("spark.shuffle.mapOutput.minSizeForBroadcast", "512k").toInt

  /** Whether to compute locality preferences for reduce tasks */
  private val shuffleLocalityEnabled = conf.getBoolean("spark.shuffle.reduceLocality.enabled", true)

  // Number of map and reduce tasks above which we do not assign preferred locations based on map
  // output sizes. We limit the size of jobs for which assign preferred locations as computing the
  // top locations by size becomes expensive.
  private val SHUFFLE_PREF_MAP_THRESHOLD = 1000
  // NOTE: This should be less than 2000 as we use HighlyCompressedMapStatus beyond that
  private val SHUFFLE_PREF_REDUCE_THRESHOLD = 1000

  // Fraction of total map output that must be at a location for it to considered as a preferred
  // location for a reduce task. Making this larger will focus on fewer locations where most data
  // can be read locally, but may lead to more delay in scheduling if those locations are busy.
  private val REDUCER_PREF_LOCS_FRACTION = 0.2

  // HashMap for storing shuffleStatuses in the driver.
  // Statuses are dropped only by explicit de-registering.
  // Exposed for testing
  val shuffleStatuses = new ConcurrentHashMap[Int, ShuffleStatus]().asScala

  private val maxRpcMessageSize = RpcUtils.maxMessageSizeBytes(conf)

  // requests for map output statuses
  private val mapOutputRequests = new LinkedBlockingQueue[GetMapOutputMessage]

  // Thread pool used for handling map output status requests. This is a separate thread pool
  // to ensure we don't block the normal dispatcher threads.
  private val threadpool: ThreadPoolExecutor = {
    val numThreads = conf.getInt("spark.shuffle.mapOutput.dispatcher.numThreads", 8)
    val pool = ThreadUtils.newDaemonFixedThreadPool(numThreads, "map-output-dispatcher")
    for (i <- 0 until numThreads) {
      pool.execute(new MessageLoop)
    }
    pool
  }

  // Make sure that we aren't going to exceed the max RPC message size by making sure
  // we use broadcast to send large map output statuses.
  if (minSizeForBroadcast > maxRpcMessageSize) {
    val msg = s"spark.shuffle.mapOutput.minSizeForBroadcast ($minSizeForBroadcast bytes) must " +
      s"be <= spark.rpc.message.maxSize ($maxRpcMessageSize bytes) to prevent sending an rpc " +
      "message that is too large."
    logError(msg)
    throw new IllegalArgumentException(msg)
  }

  def post(message: GetMapOutputMessage): Unit = {
    mapOutputRequests.offer(message)
  }

  /** Message loop used for dispatching messages. */
  private class MessageLoop extends Runnable {
    override def run(): Unit = {
      try {
        while (true) {
          try {
            val data = mapOutputRequests.take()
            if (data == PoisonPill) {
              // Put PoisonPill back so that other MessageLoops can see it.
              mapOutputRequests.offer(PoisonPill)
              return
            }
            val context = data.context
            val shuffleId = data.shuffleId
            val hostPort = context.senderAddress.hostPort
            logDebug("Handling request to send map output locations for shuffle " + shuffleId +
              " to " + hostPort)
            val shuffleStatus = shuffleStatuses.get(shuffleId).head
            context.reply(
              shuffleStatus.serializedMapStatus(broadcastManager, isLocal, minSizeForBroadcast))
          } catch {
            case NonFatal(e) => logError(e.getMessage, e)
          }
        }
      } catch {
        case ie: InterruptedException => // exit
      }
    }
  }

  /** A poison endpoint that indicates MessageLoop should exit its message loop. */
  private val PoisonPill = new GetMapOutputMessage(-99, null)

  // Used only in unit tests.
  private[spark] def getNumCachedSerializedBroadcast: Int = {
    shuffleStatuses.valuesIterator.count(_.hasCachedSerializedBroadcast)
  }

  def registerShuffle(shuffleId: Int, numMaps: Int) {
    if (shuffleStatuses.put(shuffleId, new ShuffleStatus(numMaps)).isDefined) {
      throw new IllegalArgumentException("Shuffle ID " + shuffleId + " registered twice")
    }
  }

  def registerMapOutput(shuffleId: Int, mapId: Int, status: MapStatus) {
    shuffleStatuses(shuffleId).addMapOutput(mapId, status)
  }

  /** Unregister map output information of the given shuffle, mapper and block manager */
  def unregisterMapOutput(shuffleId: Int, mapId: Int, bmAddress: BlockManagerId) {
    shuffleStatuses.get(shuffleId) match {
      case Some(shuffleStatus) =>
        shuffleStatus.removeMapOutput(mapId, bmAddress)
        incrementEpoch()
      case None =>
        throw new SparkException("unregisterMapOutput called for nonexistent shuffle ID")
    }
  }

  /** Unregister shuffle data */
  def unregisterShuffle(shuffleId: Int) {
    shuffleStatuses.remove(shuffleId).foreach { shuffleStatus =>
      shuffleStatus.invalidateSerializedMapOutputStatusCache()
    }
  }

  /**
    * Removes all shuffle outputs associated with this host. Note that this will also remove
    * outputs which are served by an external shuffle server (if one exists).
    */
  def removeOutputsOnHost(host: String): Unit = {
    shuffleStatuses.valuesIterator.foreach {
      _.removeOutputsOnHost(host)
    }
    incrementEpoch()
  }

  /**
    * Removes all shuffle outputs associated with this executor. Note that this will also remove
    * outputs which are served by an external shuffle server (if one exists), as they are still
    * registered with this execId.
    */
  def removeOutputsOnExecutor(execId: String): Unit = {
    shuffleStatuses.valuesIterator.foreach {
      _.removeOutputsOnExecutor(execId)
    }
    incrementEpoch()
  }

  /** Check if the given shuffle is being tracked */
  def containsShuffle(shuffleId: Int): Boolean = shuffleStatuses.contains(shuffleId)

  def getNumAvailableOutputs(shuffleId: Int): Int = {
    shuffleStatuses.get(shuffleId).map(_.numAvailableOutputs).getOrElse(0)
  }

  /**
    * Returns the sequence of partition ids that are missing (i.e. needs to be computed), or None
    * if the MapOutputTrackerMaster doesn't know about this shuffle.
    */
  def findMissingPartitions(shuffleId: Int): Option[Seq[Int]] = {
    shuffleStatuses.get(shuffleId).map(_.findMissingPartitions())
  }

  /**
    * Grouped function of Range, this is to avoid traverse of all elements of Range using
    * IterableLike's grouped function.
    */
  def rangeGrouped(range: Range, size: Int): Seq[Range] = {
    val start = range.start
    val step = range.step
    val end = range.end
    for (i <- start.until(end, size * step)) yield {
      i.until(i + size * step, step)
    }
  }

  /**
    * To equally divide n elements into m buckets, basically each bucket should have n/m elements,
    * for the remaining n%m elements, add one more element to the first n%m buckets each.
    */
  def equallyDivide(numElements: Int, numBuckets: Int): Seq[Seq[Int]] = {
    val elementsPerBucket = numElements / numBuckets
    val remaining = numElements % numBuckets
    val splitPoint = (elementsPerBucket + 1) * remaining
    if (elementsPerBucket == 0) {
      rangeGrouped(0.until(splitPoint), elementsPerBucket + 1)
    } else {
      rangeGrouped(0.until(splitPoint), elementsPerBucket + 1) ++
        rangeGrouped(splitPoint.until(numElements), elementsPerBucket)
    }
  }

  /**
    * Return statistics about all of the outputs for a given shuffle.
    */
  def getStatistics(dep: ShuffleDependency[_, _, _]): MapOutputStatistics = {
    shuffleStatuses(dep.shuffleId).withMapStatuses { statuses =>
      val totalSizes = new Array[Long](dep.partitioner.numPartitions)
      val parallelAggThreshold = conf.get(
        SHUFFLE_MAP_OUTPUT_PARALLEL_AGGREGATION_THRESHOLD)
      val parallelism = math.min(
        Runtime.getRuntime.availableProcessors(),
        statuses.length.toLong * totalSizes.length / parallelAggThreshold + 1).toInt

      var totalRecords = new Array[Long](0)
      val records = statuses(0).getRecordForBlock(0)

      if (parallelism <= 1) {
        for (s <- statuses) {
          for (i <- 0 until totalSizes.length) {
            totalSizes(i) += s.getSizeForBlock(i)
          }
        }
        // records == -1 means no records number info
        if (records != -1) {
          totalRecords = new Array[Long](dep.partitioner.numPartitions)
          for (s <- statuses) {
            for (i <- totalRecords.indices) {
              totalRecords(i) += s.getRecordForBlock(i)
            }
          }
        }
      } else {
        val (sizeParallelism, recordParallelism) = if (records != -1) {
          (parallelism / 2, parallelism - parallelism / 2)
        } else {
          (parallelism, 0)
        }
        val threadPool = ThreadUtils.newDaemonFixedThreadPool(parallelism, "map-output-aggregate")
        try {
          implicit val executionContext = ExecutionContext.fromExecutor(threadPool)
          var mapStatusSubmitTasks = equallyDivide(totalSizes.length, sizeParallelism).map {
            reduceIds =>
              Future {
                for (s <- statuses; i <- reduceIds) {
                  totalSizes(i) += s.getSizeForBlock(i)
                }
              }
          }
          if (records != -1) {
            totalRecords = new Array[Long](dep.partitioner.numPartitions)
            mapStatusSubmitTasks ++= equallyDivide(totalRecords.length, recordParallelism).map {
              reduceIds =>
                Future {
                  for (s <- statuses; i <- reduceIds) {
                    totalRecords(i) += s.getRecordForBlock(i)
                  }
                }
            }
          }
          ThreadUtils.awaitResult(Future.sequence(mapStatusSubmitTasks), Duration.Inf)
        } finally {
          threadPool.shutdown()
        }
      }
      new MapOutputStatistics(dep.shuffleId, totalSizes, totalRecords)
    }
  }

  /**
    * Return the preferred hosts on which to run the given map output partition in a given shuffle,
    * i.e. the nodes that the most outputs for that partition are on.
    *
    * @param dep         shuffle dependency object
    * @param partitionId map output partition that we want to read
    * @return a sequence of host names
    */
  def getPreferredLocationsForShuffle(dep: ShuffleDependency[_, _, _], partitionId: Int)
  : Seq[String] = {
    if (shuffleLocalityEnabled && dep.rdd.partitions.length < SHUFFLE_PREF_MAP_THRESHOLD &&
      dep.partitioner.numPartitions < SHUFFLE_PREF_REDUCE_THRESHOLD) {
      val blockManagerIds = getLocationsWithLargestOutputs(dep.shuffleId, partitionId,
        dep.partitioner.numPartitions, REDUCER_PREF_LOCS_FRACTION)
      if (blockManagerIds.nonEmpty) {
        blockManagerIds.get.map(_.host)
      } else {
        Nil
      }
    } else {
      Nil
    }
  }

  /**
    * Return a list of locations that each have fraction of map output greater than the specified
    * threshold.
    *
    * @param shuffleId         id of the shuffle
    * @param reducerId         id of the reduce task
    * @param numReducers       total number of reducers in the shuffle
    * @param fractionThreshold fraction of total map output size that a location must have
    *                          for it to be considered large.
    */
  def getLocationsWithLargestOutputs(
                                      shuffleId: Int,
                                      reducerId: Int,
                                      numReducers: Int,
                                      fractionThreshold: Double)
  : Option[Array[BlockManagerId]] = {

    val shuffleStatus = shuffleStatuses.get(shuffleId).orNull
    if (shuffleStatus != null) {
      shuffleStatus.withMapStatuses { statuses =>
        if (statuses.nonEmpty) {
          // HashMap to add up sizes of all blocks at the same location
          val locs = new HashMap[BlockManagerId, Long]
          var totalOutputSize = 0L
          var mapIdx = 0
          while (mapIdx < statuses.length) {
            val status = statuses(mapIdx)
            // status may be null here if we are called between registerShuffle, which creates an
            // array with null entries for each output, and registerMapOutputs, which populates it
            // with valid status entries. This is possible if one thread schedules a job which
            // depends on an RDD which is currently being computed by another thread.
            if (status != null) {
              val blockSize = status.getSizeForBlock(reducerId)
              if (blockSize > 0) {
                locs(status.location) = locs.getOrElse(status.location, 0L) + blockSize
                totalOutputSize += blockSize
              }
            }
            mapIdx = mapIdx + 1
          }
          val topLocs = locs.filter { case (loc, size) =>
            size.toDouble / totalOutputSize >= fractionThreshold
          }
          // Return if we have any locations which satisfy the required threshold
          if (topLocs.nonEmpty) {
            return Some(topLocs.keys.toArray)
          }
        }
      }
    }
    None
  }

  /**
    * Return the locations where the Mapper(s) ran. The locations each includes both a host and an
    * executor id on that host.
    *
    * @param dep        shuffle dependency object
    * @param startMapId the start map id
    * @param endMapId   the end map id
    * @return a sequence of locations that each includes both a host and an executor id on that
    *         host.
    */
  def getMapLocation(dep: ShuffleDependency[_, _, _], startMapId: Int, endMapId: Int): Seq[String] = {
    val shuffleStatus = shuffleStatuses.get(dep.shuffleId).orNull
    if (shuffleStatus != null) {
      shuffleStatus.withMapStatuses { statuses =>
        if (startMapId >= 0 && endMapId <= statuses.length) {
          val statusesPicked = statuses.slice(startMapId, endMapId).filter(_ != null)
          statusesPicked.map { status =>
            ExecutorCacheTaskLocation(status.location.host, status.location.executorId).toString
          }
        } else {
          Nil
        }
      }
    } else {
      Nil
    }
  }

  def incrementEpoch() {
    epochLock.synchronized {
      epoch += 1
      logDebug("Increasing epoch to " + epoch)
    }
  }

  /** Called to get current epoch number. */
  def getEpoch: Long = {
    epochLock.synchronized {
      return epoch
    }
  }

  // This method is only called in local-mode.
  def getMapSizesByExecutorId(
                               shuffleId: Int,
                               startPartition: Int,
                               endPartition: Int,
                               serializerRelocatable: Boolean): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    logDebug(s"Fetching outputs for shuffle $shuffleId, partitions $startPartition-$endPartition")
    shuffleStatuses.get(shuffleId) match {
      case Some(shuffleStatus) =>
        shuffleStatus.withMapStatuses { statuses =>
          MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, statuses,
            supportsContinuousBlockBatchFetch(serializerRelocatable))}
      case None =>
        Seq.empty
    }
  }

  override def getMapSizesByExecutorId(
                                        shuffleId: Int,
                                        startPartition: Int,
                                        endPartition: Int,
                                        startMapId: Int,
                                        endMapId: Int,
                                        serializerRelocatable: Boolean): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    logDebug(s"Fetching outputs for shuffle $shuffleId, startMapId $startMapId endMapId $endMapId" +
      s"partitions $startPartition-$endPartition")
    shuffleStatuses.get(shuffleId) match {
      case Some(shuffleStatus) =>
        shuffleStatus.withMapStatuses { statuses =>
          MapOutputTracker.convertMapStatuses(
            shuffleId,
            startPartition,
            endPartition,
            statuses,
            startMapId,
            endMapId,
            supportsContinuousBlockBatchFetch(serializerRelocatable))
        }
      case None =>
        Seq.empty
    }
  }

  override def stop() {
    mapOutputRequests.offer(PoisonPill)
    threadpool.shutdown()
    sendTracker(StopMapOutputTracker)
    trackerEndpoint = null
    shuffleStatuses.clear()
  }

  override def getUpdatedStatus(shuffleId: Int, startPartition: Int, endPartition: Int,
                                startMapId: Int, endMapId: Int): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    logInfo(" ask master getUpdatedStatus")
    return new ArrayBuffer[(BlockManagerId, Seq[(BlockId, Long)])]()
  }
}

/**
  * Executor-side client for fetching map output info from the driver's MapOutputTrackerMaster.
  * Note that this is not used in local-mode; instead, local-mode Executors access the
  * MapOutputTrackerMaster directly (which is possible because the master and worker share a comon
  * superclass).
  */
private[spark] class MapOutputTrackerWorker(conf: SparkConf) extends MapOutputTracker(conf) {

  val mapStatuses: Map[Int, Array[MapStatus]] =
    new ConcurrentHashMap[Int, Array[MapStatus]]().asScala

  /** Remembers which map output locations are currently being fetched on an executor. */
  private val fetching = new HashSet[Int]

  override def getMapSizesByExecutorId(
                                        shuffleId: Int,
                                        startPartition: Int,
                                        endPartition: Int,
                                        serializerRelocatable: Boolean): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    logDebug(s"Fetching outputs for shuffle $shuffleId, partitions $startPartition-$endPartition")
    val statuses = getStatuses(shuffleId)
    try {
      MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, statuses,
        supportsContinuousBlockBatchFetch(serializerRelocatable))
    } catch {
      case e: MetadataFetchFailedException =>
        // We experienced a fetch failure so our mapStatuses cache is outdated; clear it:
        mapStatuses.clear()
        throw e
    }
  }

  override def getMapSizesByExecutorId(
                                        shuffleId: Int,
                                        startPartition: Int,
                                        endPartition: Int,
                                        startMapId: Int,
                                        endMapId: Int,
                                        serializerRelocatable: Boolean): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    logDebug(s"Fetching outputs for shuffle $shuffleId, startMapId $startMapId endMapId $endMapId" +
      s"partitions $startPartition-$endPartition")
    val statuses = getStatuses(shuffleId)
    try {
      MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, statuses,
        startMapId, endMapId, supportsContinuousBlockBatchFetch(serializerRelocatable))
    } catch {
      case e: MetadataFetchFailedException =>
        // We experienced a fetch failure so our mapStatuses cache is outdated; clear it:
        mapStatuses.clear()
        throw e
    }
  }

  /**
    * Get or fetch the array of MapStatuses for a given shuffle ID. NOTE: clients MUST synchronize
    * on this array when reading it, because on the driver, we may be changing it in place.
    *
    * (It would be nice to remove this restriction in the future.)
    */
  private def getStatuses(shuffleId: Int): Array[MapStatus] = {
    val statuses = mapStatuses.get(shuffleId).orNull
    if (statuses == null) {
      logInfo("Don't have map outputs for shuffle " + shuffleId + ", fetching them")
      val startTime = System.currentTimeMillis
      var fetchedStatuses: Array[MapStatus] = null
      fetching.synchronized {
        // Someone else is fetching it; wait for them to be done
        while (fetching.contains(shuffleId)) {
          try {
            fetching.wait()
          } catch {
            case e: InterruptedException =>
          }
        }

        // Either while we waited the fetch happened successfully, or
        // someone fetched it in between the get and the fetching.synchronized.
        fetchedStatuses = mapStatuses.get(shuffleId).orNull
        if (fetchedStatuses == null) {
          // We have to do the fetch, get others to wait for us.
          fetching += shuffleId
        }
      }

      if (fetchedStatuses == null) {
        // We won the race to fetch the statuses; do so
        logInfo("Doing the fetch; tracker endpoint = " + trackerEndpoint)
        // This try-finally prevents hangs due to timeouts:
        try {
          val fetchedBytes = askTracker[Array[Byte]](GetMapOutputStatuses(shuffleId))
          fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes)
          logInfo("Got the output locations")
          mapStatuses.put(shuffleId, fetchedStatuses)
        } finally {
          fetching.synchronized {
            fetching -= shuffleId
            fetching.notifyAll()
          }
        }
      }
      logDebug(s"Fetching map output statuses for shuffle $shuffleId took " +
        s"${System.currentTimeMillis - startTime} ms")

      if (fetchedStatuses != null) {
        fetchedStatuses
      } else {
        logError("Missing all output locations for shuffle " + shuffleId)
        throw new MetadataFetchFailedException(
          shuffleId, -1, "Missing all output locations for shuffle " + shuffleId)
      }
    } else {
      statuses
    }
  }


  /** Unregister shuffle data. */
  def unregisterShuffle(shuffleId: Int): Unit = {
    mapStatuses.remove(shuffleId)
  }

  /**
    * Called from executors to update the epoch number, potentially clearing old outputs
    * because of a fetch failure. Each executor task calls this with the latest epoch
    * number on the driver at the time it was created.
    */
  def updateEpoch(newEpoch: Long): Unit = {
    epochLock.synchronized {
      if (newEpoch > epoch) {
        logInfo("Updating epoch to " + newEpoch + " and clearing cache")
        epoch = newEpoch
        mapStatuses.clear()
      }
    }
  }

  override def getUpdatedStatus(shuffleId: Int, startPartition: Int,
                                endPartition: Int,
                                startMapId: Int, endMapId: Int)
  : Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    //    if(startMapId == -1 && endMapId == -1) return new ArrayBuffer[(BlockManagerId, Seq[(BlockId, Long)])]();
    val statuses = mapStatuses.get(shuffleId).orNull
    if (statuses == null) {
      logInfo("yang:Don't have map outputs for shuffle " + shuffleId + ", fetching them")
      var fetchedStatuses: Array[MapStatus] = null
      fetching.synchronized {
        if (fetching.contains(shuffleId)) {
          // Someone else is fetching it; wait for them to be done
          while (fetching.contains(shuffleId)) {
            try {
              fetching.wait()
            } catch {
              case e: InterruptedException =>
            }
          }
        }

        // Either while we waited the fetch happened successfully, or
        // someone fetched it in between the get and the fetching.synchronized.
        fetchedStatuses = mapStatuses.get(shuffleId).orNull
        if (fetchedStatuses == null) {
          // We have to do the fetch, get others to wait for us.
          fetching += shuffleId
        }
      }

      if (fetchedStatuses == null) {
        // We won the race to fetch the output locs; do so
        logInfo("yang:Doing the fetch; tracker actor = " + trackerEndpoint)
        // This try-finally prevents hangs due to timeouts:
        try {
          val fetchedBytes =
            askTracker(GetMapOutputStatuses(shuffleId)).asInstanceOf[Array[Byte]]
          fetchedStatuses = MapOutputTracker.deserializeMapStatuses(fetchedBytes)
          logInfo("yang:Got the output locations")
        } finally {
          fetching.synchronized {
            fetching -= shuffleId
            fetching.notifyAll()
          }
        }
      }
      if (fetchedStatuses != null) {
        var isMapFinished: Boolean = true
        var newFetchedStatuses: ArrayBuffer[MapStatus] = new ArrayBuffer[MapStatus]()
        fetchedStatuses.synchronized {
          fetchedStatuses.map {
            sta =>
              if (sta == null) {
                isMapFinished = false

              } else {
                //                newFetchedStatuses.append(sta)
              }
          }
          if (isMapFinished) {
            mapStatuses.put(shuffleId, fetchedStatuses)
          }
          val newFetchedStatusesArray = newFetchedStatuses.toArray
          fetching.synchronized {
            fetching -= shuffleId
            fetching.notifyAll()
          }
          if (startMapId == -1 && endMapId == -1) {
            if (fetchedStatuses.size == 0) return null
            return MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, fetchedStatuses, false)
          }
          else {
            //            if (fetchedStatuses.slice(startMapId,endMapId+1).contains(null)) return null
            return MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, fetchedStatuses, startMapId, endMapId, false)
          }
        }
      }
      else {
        logInfo(shuffleId + startPartition + "in getUpdatedStatus Misssing all output location for shuffle" + shuffleId)
        null
      }
    }
    else {
      if (startMapId == -1 && endMapId == -1)
        return MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, statuses, false)
      else return MapOutputTracker.convertMapStatuses(shuffleId, startPartition, endPartition, statuses, startMapId, endMapId, false)
    }
  }
}


private[spark] object MapOutputTracker extends Logging {

  val ENDPOINT_NAME = "MapOutputTracker"
  private val DIRECT = 0
  private val BROADCAST = 1

  // Serialize an array of map output locations into an efficient byte format so that we can send
  // it to reduce tasks. We do this by compressing the serialized bytes using GZIP. They will
  // generally be pretty compressible because many map outputs will be on the same hostname.
  def serializeMapStatuses(statuses: Array[MapStatus], broadcastManager: BroadcastManager,
                           isLocal: Boolean, minBroadcastSize: Int): (Array[Byte], Broadcast[Array[Byte]]) = {
    val out = new ByteArrayOutputStream
    out.write(DIRECT)
    val objOut = new ObjectOutputStream(new GZIPOutputStream(out))
    Utils.tryWithSafeFinally {
      // Since statuses can be modified in parallel, sync on it
      statuses.synchronized {
        objOut.writeObject(statuses)
      }
    } {
      objOut.close()
    }
    val arr = out.toByteArray
    if (arr.length >= minBroadcastSize) {
      // Use broadcast instead.
      // Important arr(0) is the tag == DIRECT, ignore that while deserializing !
      val bcast = broadcastManager.newBroadcast(arr, isLocal)
      // toByteArray creates copy, so we can reuse out
      out.reset()
      out.write(BROADCAST)
      val oos = new ObjectOutputStream(new GZIPOutputStream(out))
      oos.writeObject(bcast)
      oos.close()
      val outArr = out.toByteArray
      logInfo("Broadcast mapstatuses size = " + outArr.length + ", actual size = " + arr.length)
      (outArr, bcast)
    } else {
      (arr, null)
    }
  }

  // Opposite of serializeMapStatuses.
  def deserializeMapStatuses(bytes: Array[Byte]): Array[MapStatus] = {
    assert(bytes.length > 0)

    def deserializeObject(arr: Array[Byte], off: Int, len: Int): AnyRef = {
      val objIn = new ObjectInputStream(new GZIPInputStream(
        new ByteArrayInputStream(arr, off, len)))
      Utils.tryWithSafeFinally {
        objIn.readObject()
      } {
        objIn.close()
      }
    }

    bytes(0) match {
      case DIRECT =>
        deserializeObject(bytes, 1, bytes.length - 1).asInstanceOf[Array[MapStatus]]
      case BROADCAST =>
        // deserialize the Broadcast, pull .value array out of it, and then deserialize that
        val bcast = deserializeObject(bytes, 1, bytes.length - 1).
          asInstanceOf[Broadcast[Array[Byte]]]
        logInfo("Broadcast mapstatuses size = " + bytes.length +
          ", actual size = " + bcast.value.length)
        // Important - ignore the DIRECT tag ! Start from offset 1
        deserializeObject(bcast.value, 1, bcast.value.length - 1).asInstanceOf[Array[MapStatus]]
      case _ => throw new IllegalArgumentException("Unexpected byte tag = " + bytes(0))
    }
  }

  /**
    * Given an array of map statuses and a range of map output partitions, returns a sequence that,
    * for each block manager ID, lists the shuffle block IDs and corresponding shuffle block sizes
    * stored at that block manager.
    *
    * If any of the statuses is null (indicating a missing location due to a failed mapper),
    * throws a FetchFailedException.
    *
    * @param shuffleId                         Identifier for the shuffle
    * @param startPartition                    Start of map output partition ID range (included in range)
    * @param endPartition                      End of map output partition ID range (excluded from range)
    * @param statuses                          List of map statuses, indexed by map ID.
    * @param supportsContinuousBlockBatchFetch if true, merge contiguous partitions in one IO
    * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
    *         and the second item is a sequence of (shuffle block ID, shuffle block size) tuples
    *         describing the shuffle blocks that are stored at that block manager.
    */
  def convertMapStatuses(
                          shuffleId: Int,
                          startPartition: Int,
                          endPartition: Int,
                          statuses: Array[MapStatus],
                          supportsContinuousBlockBatchFetch: Boolean): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    assert(statuses != null)
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
    val removeStageBarrier= SparkEnv.get.conf.getBoolean("spark.shuffle.removeStageBarrier",false)
    if (removeStageBarrier){
      for ((status, mapId) <- statuses.zipWithIndex) {
        if (status == null) {

        } else {
          if (endPartition - startPartition > 1 && supportsContinuousBlockBatchFetch) {
            val totalSize: Long = (startPartition until endPartition).map(status.getSizeForBlock).sum
            splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
              ((ContinuousShuffleBlockId(shuffleId, mapId,
                startPartition, endPartition - startPartition), totalSize))
          } else {
            for (part <- startPartition until endPartition) {
              splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
                ((ShuffleBlockId(shuffleId, mapId, part), status.getSizeForBlock(part)))
            }
          }
        }
      }

      splitsByAddress.toSeq
    }
    else{
    for ((status, mapId) <- statuses.zipWithIndex) {
      if (status == null) {
        val errorMessage = s"Missing an output location for shuffle $shuffleId"
        logError(errorMessage)
        throw new MetadataFetchFailedException(shuffleId, startPartition, errorMessage)
      } else {
        if (endPartition - startPartition > 1 && supportsContinuousBlockBatchFetch) {
          val totalSize: Long = (startPartition until endPartition).map(status.getSizeForBlock).sum
          splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
            ((ContinuousShuffleBlockId(shuffleId, mapId,
              startPartition, endPartition - startPartition), totalSize))
        } else {
          for (part <- startPartition until endPartition) {
            splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
              ((ShuffleBlockId(shuffleId, mapId, part), status.getSizeForBlock(part)))
          }
        }
      }
    }

    splitsByAddress.toSeq
  }
  }

  /**
    * Given an array of map statuses, the start map Id, end map Id and a range of map output
    * partitions, returns a sequence that, lists the shuffle block IDs and corresponding shuffle
    * block sizes stored at that block manager.
    *
    * If the status of the map is null (indicating a missing location due to a failed mapper),
    * throws a FetchFailedException.
    *
    * @param shuffleId      Identifier for the shuffle
    * @param startPartition Start of map output partition ID range (included in range)
    * @param endPartition   End of map output partition ID range (excluded from range)
    * @param statuses       List of map statuses, indexed by map ID.
    * @param startMapId     Start of map Id range (included in range)
    * @param endMapId       End of map Id (excluded from range)
    * @return A sequence of 2-item tuples, where the first item in the tuple is a BlockManagerId,
    *         and the second item is a sequence of (shuffle block ID, shuffle block size) tuples
    *         describing the shuffle blocks that are stored at that block manager.
    */
  def convertMapStatuses(
                          shuffleId: Int,
                          startPartition: Int,
                          endPartition: Int,
                          statuses: Array[MapStatus],
                          startMapId: Int,
                          endMapId: Int,
                          supportsContinuousBlockBatchFetch: Boolean): Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    assert(statuses != null && statuses.length >= endMapId && startMapId >= 0)
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
    val removeStageBarrier = SparkEnv.get.conf.getBoolean("spark.shuffle.removeStageBarrier", false)
    if (removeStageBarrier) {
      assert(statuses != null && statuses.length >= endMapId && startMapId >= 0)
      val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(BlockId, Long)]]
      for (mapId <- startMapId until endMapId) {
        val status = statuses(mapId)
        if (status == null) {
          for ((status, mapId) <- statuses.zipWithIndex) {
            if (status == null) {
            }
            else {
              for (part <- startPartition until endPartition) {
                splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
                  ((ShuffleBlockId(shuffleId, mapId, part), status.getSizeForBlock(part)))
              }
            }
          }

          splitsByAddress.toSeq
        } else {
          for (part <- startPartition until endPartition) {
            splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
              ((ShuffleBlockId(shuffleId, mapId, part), status.getSizeForBlock(part)))
          }
        }
      }
      splitsByAddress.toSeq
    }
    else {
      for (mapId <- startMapId until endMapId) {
        val status = statuses(mapId)
        if (status == null) {
          val errorMessage = s"Missing an output location for shuffle $shuffleId"
          logError(errorMessage)
          throw new MetadataFetchFailedException(shuffleId, startPartition, errorMessage)
        } else {
          if (endPartition - startPartition > 1 && supportsContinuousBlockBatchFetch) {
            val totalSize: Long = (startPartition until endPartition).map(status.getSizeForBlock).sum
            splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
              ((ContinuousShuffleBlockId(shuffleId, mapId,
                startPartition, endPartition - startPartition), totalSize))
          } else {
            for (part <- startPartition until endPartition) {
              splitsByAddress.getOrElseUpdate(status.location, ArrayBuffer()) +=
                ((ShuffleBlockId(shuffleId, mapId, part), status.getSizeForBlock(part)))
            }
          }
        }
      }
      splitsByAddress.toSeq
    }
  }
}
