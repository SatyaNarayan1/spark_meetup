import org.apache.spark.{SparkEnv, SparkContext, SparkConf}
import org.apache.spark.storage.RDDBlockId


object CacheExample {
  def main(args:Array[String]) {
    val conf = new SparkConf().setAppName("Lineage-Demo").setMaster(args(0))
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.textFile(args(1))

    val wordCount = rdd.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    val firstPartition = wordCount.partitions.head

    wordCount.cache()

    //check before action
    println("Check Persistent RDD List before Action : " + sc.getPersistentRDDs)

    val blockManager = SparkEnv.get.blockManager

    val blockId = RDDBlockId(wordCount.id, firstPartition.index)

    println("Check Block Manager before action : " + blockManager.get(blockId))
    //Perform an action
    wordCount.count

    //check before action
    println("Check Persistent RDD List after Action  " + sc.getPersistentRDDs)
    println("Check Block Manager before action" + blockManager.get(blockId))
  }

}
