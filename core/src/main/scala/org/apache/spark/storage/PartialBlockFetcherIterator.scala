package org.apache.spark.storage
import java.io.InputStream
import org.apache.spark.{ SparkEnv, TaskContext}
import org.apache.spark.internal.{Logging, config}
import scala.collection.mutable.{ArrayBuffer, HashSet}
import org.apache.spark.network.shuffle.ShuffleClient

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
    SparkEnv.get.conf.getInt("spark.reducer.mapOutput.fetchInterval", 1000)

  private var iterator: Iterator[(BlockId, InputStream)] = null

  // Track the map outputs we've delegated
  private val delegatedStatuses = new HashSet[String]()

  private var fetchTime: Int = 1

  private var statuses: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = null

  private var notNullStatuses: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = null

  private var nextNum: Int = 0

  statuses = SparkEnv.get.mapOutputTracker.getUpdatedStatus(shuffleId, startPartition,endPartition,startMapId.getOrElse(-1),endMapId.getOrElse(-1))

  initialize()

  // Get the updated map output
  private def updateStatuses() {
    fetchTime += 1

    statuses = SparkEnv.get.mapOutputTracker.getUpdatedStatus(shuffleId, startPartition,endPartition,startMapId.getOrElse(-1),endMapId.getOrElse(-1))

  }


  private def readyStatuses: Seq[(BlockManagerId, Seq[(BlockId, Long)])] = {
    if(statuses == null)  statuses
    else {
      val newStatuses = statuses
      newStatuses.filter(_._1 != null)
      newStatuses.foreach(x => x._2.filter(_._1 != null))
      newStatuses
    }
  }
  // Check if there's new map outputs available
  private def newStatusesReady = {
    if(readyStatuses == null || readyStatuses.size == 0) false
    else hashExtraBlock(delegatedStatuses,readyStatuses)

  }
  //Check if there's new new block available
  private def hashExtraBlock(delegatedStatuses: HashSet[String],
                             readyStatuses: Seq[(BlockManagerId, Seq[(BlockId, Long)])]): Boolean={
    readyStatuses.foreach( x =>
      x._2.foreach(y =>
        if (!delegatedStatuses.contains(x._1 + y._1.toString + y._2)){
          return true
        }
      )
    )
    false
  }

  private def readyStatusesToIndex(readyStatuses: Seq[(BlockManagerId, Seq[(BlockId, Long)])]):Seq[String] = {
    val res = new ArrayBuffer[String]()
    readyStatuses.foreach(x =>{
      x._2.foreach( y => {
        res.append(x._1 + y._1.toString + y._2)
      })
    })
    res
  }

  private def getIterator() = {
    while (!newStatusesReady) {
      logInfo("nothing to fetch sleep in partialIterator")
      Thread.sleep(mapOutputFetchInterval)
      updateStatuses()
    }

    for (index <- readyStatusesToIndex(readyStatuses) if !delegatedStatuses.contains(index)){
      delegatedStatuses += index
    }

    logInfo("Delegating " + statuses.map(_._2.size).sum +
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
      var i = 1
      for ( i <- 1 to nextNum){
        iterator.next()
      }
      if (iterator.hasNext) {
        return true
      }
    }
    false
  }

  override def next(): (BlockId, InputStream) = {
    nextNum = nextNum+1
    return iterator.next()
  }
}
