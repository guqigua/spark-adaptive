package org.apache.spark.storage

import java.util.concurrent.LinkedBlockingQueue

import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import scala.util.Try

import org.apache.spark.{SparkEnv, MapOutputTracker, Logging, TaskContext}
import org.apache.spark.network.shuffle.ShuffleClient
import org.apache.spark.serializer.Serializer

private[spark]
class PartialBlockFetcherIterator(
                                   context: TaskContext,
                                   shuffleClient: ShuffleClient,
                                   blockManager: BlockManager,
                                   var statuses: Array[(BlockManagerId, Long)],
                                   serializer: Serializer,
                                   shuffleId: Int,
                                   reduceId: Int)
  extends Iterator[(BlockId, Try[Iterator[Any]])] with Logging {

  private val mapOutputFetchInterval =
    SparkEnv.get.conf.getInt("spark.reducer.mapOutput.fetchInterval", 1000)

  private var iterator: Iterator[(BlockId, Try[Iterator[Any]])] = null

  // Track the map outputs we've delegated
  private val delegatedStatuses = new HashSet[Int]()

  private var fetchTime: Int = 1

  initialize()

  // Get the updated map output
  private def updateStatuses() {
    fetchTime += 1
    logDebug("Still missing " + statuses.filter(_ == null).size + " map outputs for reduce "
      + reduceId + " of shuffle " + shuffleId + " next fetchTime=" + fetchTime)
    val update = SparkEnv.get.mapOutputTracker.getUpdatedStatus(shuffleId, reduceId)
    statuses = update
  }

  private def readyStatuses = (0 until statuses.size).filter(statuses(_) != null)

  // Check if there's new map outputs available
  private def newStatusesReady = readyStatuses.exists(!delegatedStatuses.contains(_))

  private def getIterator() = {
    while (!newStatusesReady) {
      Thread.sleep(mapOutputFetchInterval)
      updateStatuses()
    }
    val splitsByAddress = new HashMap[BlockManagerId, ArrayBuffer[(Int, Long)]]
    for (index <- readyStatuses if !delegatedStatuses.contains(index)) {
      splitsByAddress.getOrElseUpdate(statuses(index)._1, ArrayBuffer()) +=
        ((index, statuses(index)._2))
      delegatedStatuses += index
