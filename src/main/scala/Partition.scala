/**
 * Created by psatya on 08/10/15.
 */
import org.apache.spark.{SparkContext,SparkConf}


object Partition {

  def main (args:Array[String]) {
    val conf = new SparkConf().setAppName("Partition-Demo").setMaster(args(0))
    val sc: SparkContext = new SparkContext(conf)

  }
}
