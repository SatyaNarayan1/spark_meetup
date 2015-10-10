/**
 * Created by psatya on 10/10/15.
 */
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf, TaskContext, Partition}



class AlphabetRDD(prev:RDD[String]) extends RDD[String](prev){
  def checkAlpha(str:String): Boolean =
  {
    if (str.size<=0)
      return false

    val x =  str.charAt(0)
    x.toUpper.toInt >=65 &&  x.toUpper.toInt<=90
  }

  override def compute(split: Partition, context: TaskContext): Iterator[String] =  {
    firstParent[String].iterator(split, context)
      .flatMap(line => line.split(" "))
      .filter(checkAlpha(_))

  }

  override protected def getPartitions: Array[Partition] = firstParent[String].partitions

}


object CustomRDD {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setAppName("Demo-CustomOperator")
      .setMaster(args(0))

    val sc: SparkContext = new SparkContext(conf)

    val rdd = sc.textFile(args(1))
    println("total number of records in rdd :"+rdd.count)
    val alphabetRDD = new AlphabetRDD(rdd)
    println("total number of records in custom rdd :"+alphabetRDD.count)

  }
}