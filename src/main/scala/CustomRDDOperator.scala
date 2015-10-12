

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD

object RDDImplicits {

  //Implicit class
  implicit class RichRDD(rdd:RDD[String])
  {

    def wordCount = {
      rdd.flatMap(line => line.split(" "))
        .map(word => (word, 1))
       .reduceByKey((value1,value2) =>value1+value2)
    }

   def countLineWith(pattern:String):Long = {
     rdd.filter(_.contains(pattern)).count
   }

  }
}


import RDDImplicits._
object CustomRDDOperator {


  def main (args:Array[String]): Unit =
  {
    val conf:SparkConf = new SparkConf()
      .setAppName("Demo-CustomOperator")
      .setMaster(args(0))

    val sc:SparkContext = new SparkContext(conf)

    val rdd = sc.textFile(args(1))
    println(rdd.count)

    val wordCountRDD = rdd.wordCount

    wordCountRDD.collect.foreach(println)

    println(rdd.countLineWith("Spark"))



  }

}
