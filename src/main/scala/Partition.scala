/**
 * Created by psatya on 08/10/15.
 */


import org.apache.spark.{Partitioner, SparkContext, SparkConf}


class FirstCharPartitioner extends Partitioner{
  override def numPartitions:Int =27

  override def  getPartition(key:Any):Int = {
    key match {
      case x:String => {
        if (x.size <= 0) {return 0}
        val n = x.charAt(0).toUpper.toInt
        if (n <= 90 && n >= 65)
          return (n - 65) % numPartitions
        else
          return 0

      }
      case _ => 0

    }
  }


}

object Partition {

  def main (args:Array[String]) {
    val conf = new SparkConf().setAppName("Partition-Demo").setMaster(args(0))
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.textFile(args(1))
    val wordCount = rdd.flatMap(_.split(" "))
      .map(word=>(word,1))
    wordCount.count()

    // Invoke custom partitioner
   val partitionGroup= wordCount.groupByKey(new FirstCharPartitioner)

    val groupedDataWithPartition = partitionGroup.mapPartitionsWithIndex{
      case(partitionNo,iterator) => {
        List((partitionNo,iterator.toList.length)).iterator
      }
    }

    groupedDataWithPartition.collect().foreach(println)
  }
}
