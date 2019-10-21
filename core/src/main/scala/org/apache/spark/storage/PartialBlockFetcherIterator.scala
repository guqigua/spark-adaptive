package org.apache.spark.storage

import java.io.InputStream

import org.apache.spark.{MapOutputTracker, SparkEnv, TaskContext}

import org.apache.spark.internal.{Logging, config}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Try
import org.apache.spark.network.shuffle.ShuffleClient
import org.apache.spark.serializer.Serializer

private[spark]
class PartialBlockFetcherIterator(
                                   context: TaskContext,
                                   shuffleClient: ShuffleClient,
                                   blockManager: BlockManager,
                                   startPartition: Int,
                                   endPartition: Int,
                                   startMapId: Option[Int] = None,
                                   endMapId: Option[Int] = None,
                                   streamWrapper: (BlockId, InputStream) => InputStream,
                                   shuffleId: Int
                                   )
  extends Iterator[(BlockId, InputStream)] with Logging {

  private val mapOutputFetchInterval =
    SparkEnv.get.conf.getInt("spark.reducer.mapOutput.fetchInterval", 10000)

  private var iterator: Iterator[(BlockId, InputStream)] = null

  // Track the map outputs we've delegated
  private val delegatedStatuses = new HashSet[Int]()

  private var fetchTime: Int = 1

  private var statuses: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = null

  private var notNullStatuses: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = null

  log.info("jiang first start PartReader")
  statuses = SparkEnv.get.mapOutputTracker.getUpdatedStatus(shuffleId, startPartition,endPartition,startMapId.getOrElse(-1),endMapId.getOrElse(-1))

  initialize()

  // Get the updated map output
  private def updateStatuses() {
    fetchTime += 1

    statuses = SparkEnv.get.mapOutputTracker.getUpdatedStatus(shuffleId, startPartition,endPartition,startMapId.getOrElse(-1),endMapId.getOrElse(-1))

  }


  private def readyStatuses = {
    if(statuses == null)  null
    else {
      var ints = (0 until statuses.size).filter(statuses(_) != null)
      ints = (0 until statuses.size).filter(statuses(_)._1 != null)
      ints = (0 until statuses.size).filter(statuses(_)._2 != null)
      ints = (0 until statuses.size).filter(statuses(_)._2(1) != null)
      ints = (0 until statuses.size).filter(statuses(_)._2(2) != null)

      if (ints == null || ints.size == 0) null
      else ints

    }
  }
  // Check if there's new map outputs available
  private def newStatusesReady = {
    if(readyStatuses == null) false
    else readyStatuses.exists(!delegatedStatuses.contains(_))
  }

  private def getIterator() = {
    while (!newStatusesReady) {
      Thread.sleep(mapOutputFetchInterval)
      updateStatuses()
    }

//    val splitByAddress = new HashMap[BlockManagerId,ArrayBuffer[(Int,Seq[(BlockId, Long)])]]

    for (index <- readyStatuses if !delegatedStatuses.contains(index)){
//      splitByAddress.getOrElseUpdate(statuses(index)._1,ArrayBuffer()) += ((index,statuses(index)._2))
      delegatedStatuses += index
    }

//    val afterStatus = Seq[(BlockManagerId, Seq[(BlockId, Long)])] = splitByAddress.toSeq.map{
//      case(addr,split) =>
//        (addr,split.map(s => BlockId()))
//    }

    logDebug("Delegating " + statuses.map(_._2.size).sum +
      " blocks to a new iterator for reduce "  + " of shuffle " + shuffleId)

    val blockFetcherItr = new ShuffleBlockFetcherIterator(
      context,
      blockManager.shuffleClient,
      blockManager,
      statuses,
      SparkEnv.get.serializerManager.wrapStream,
      // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
      SparkEnv.get.conf.getSizeAsMb("spark.reducer.maxSizeInFlight", "48m") * 1024 * 1024,
      SparkEnv.get.conf.getInt("spark.reducer.maxReqsInFlight", Int.MaxValue),
      SparkEnv.get.conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
      SparkEnv.get.conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
      SparkEnv.get.conf.getBoolean("spark.shuffle.detectCorrupt", true))
    blockFetcherItr
  }

  private[this] def initialize(){
    iterator = getIterator()
  }

  override def hasNext: Boolean = {
    // Firstly see if the delegated iterators have more blocks for us
    if (iterator.hasNext) {
      return true
    }
    // If we have blocks not delegated yet, try to delegate them to a new iterator
    // and depend on the iterator to tell us if there are valid blocks.
    while (delegatedStatuses.size < statuses.size) {
      iterator = getIterator()
      if(iterator == null) return false
      if (iterator.hasNext) {
        return true
      }
    }
    false
  }

  override def next(): (BlockId, InputStream) = {
    return iterator.next()
  }
}
