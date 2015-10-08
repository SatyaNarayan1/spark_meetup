/**
 * Created by psatya on 08/10/15.
 */

import org.apache.spark.{SparkContext,SparkConf}

object Lineage {
 def main (args:Array[String]) {
   val conf = new SparkConf().setAppName("Lineage-Demo").setMaster(args(0))
   val sc: SparkContext = new SparkContext(conf)
  println( sc.toString)
 }
}
