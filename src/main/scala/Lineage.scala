/**
 * Created by psatya on 08/10/15.
 */

import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.rdd.RDD


object Lineage {

  def getDependency[T](rdd:RDD[T]): String =
  {
    rdd.dependencies.head.rdd.getClass.getSimpleName
  }

 def main (args:Array[String]) {

   val conf = new SparkConf().setAppName("Lineage-Demo").setMaster(args(0))
   val sc: SparkContext = new SparkContext(conf)

   val rdd = sc.textFile(args(1))
   println("Parent for rdd = "+getDependency(rdd) )

   val wordRDD = rdd.flatMap(line=>line.split(" "))
   println("Parent for wordRDD = "+getDependency(wordRDD) )

   val wordMap=  wordRDD.map(word => (word,1))
   println("Parent for wordMap = "+getDependency(wordMap) )

   val wordCount =  wordMap.reduceByKey(_+_)
   println("Parent for wordCount = "+getDependency(wordCount) )
    wordCount.collect.foreach(println)


 }
}
