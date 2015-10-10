/**
 * Created by psatya on 08/10/15.
 */


import org.apache.spark.{Partitioner, SparkContext, SparkConf}


class FirstCharPartitioner extends Partitioner{
  override def numPartitions:Int =26

  override def  getPartition(key:Any):Int = {
    key match {
      case x:String => (x.charAt(0).toUpper.toInt - 65) % numPartitions
      case _ => 0

    }
  }

}

object Partition {
def checkAlpha(str:String): Boolean =
  {
    if (str.size<=0)
      return false

    val x =  str.charAt(0)
    x.toUpper.toInt >=65 &&  x.toUpper.toInt<=90
  }

  def main (args:Array[String]) {
    val conf = new SparkConf().setAppName("Partition-Demo").setMaster(args(0))
    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.textFile(args(1))
    val wordCount = rdd.flatMap(_.split(" "))
      .filter(checkAlpha(_) )
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
