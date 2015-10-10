import org.apache.spark.{SparkConf,SparkContext}
val conf = new SparkConf().setAppName("Demo").setMaster(args(0))
val sc: SparkContext = new SparkContext(conf)

//lineage
sc.textFile("/Users/psatya/git/spark_meetup/src/main/resources/Readme.md")
  .flatMap(line=>line.split(" "))
  .map(word => (word,1))
  .reduceByKey(_+_)
  .collect()

//Shuffle
sc.textFile("/Users/psatya/Downloads/part-00000")
  .flatMap(line=>line.split(" "))
  .map(word => (word,1))
  .reduceByKey(_+_)
  .count

sc.textFile("/Users/psatya/Downloads/part-00000")
  .flatMap(line=>line.split(" "))
  .map(word => (word,1))
  .groupByKey()
  .map(row=>(row._1,row._2.sum))
  .count



//lineage

import org.apache.spark.rdd.RDD

def getDependency[T](rdd:RDD[T]): String =
{
  rdd.dependencies.head.rdd.getClass.getSimpleName
}
val rdd = sc.textFile("/Users/psatya/git/spark_meetup/src/main/resources/Readme.md")
println("Parent for rdd = "+getDependency(rdd) )

val wordRDD = rdd.flatMap(line=>line.split(" "))
println("Parent for wordRDD = "+getDependency(wordRDD) )

val wordMap=  wordRDD.map(word => (word,1))
println("Parent for wordMap = "+getDependency(wordMap) )

val wordCount =  wordMap.reduceByKey(_+_)
println("Parent for wordCount = "+getDependency(wordMap) )
wordCount.collect
