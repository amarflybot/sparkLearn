import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by amarendra on 23/10/16.
  */
object WordCount {

  def main(args: Array[String]){

    val conf = new SparkConf().setAppName("Word Count").setMaster("spark://Apples-MacBook-Pro.local:7077")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile("file:///Users/amarendra/Documents/spark-2.0.7/README.md")
    val tokenizedFileData = textFile.flatMap(line=>line.split(" "))
    val countPrep = tokenizedFileData.map(word=>(word,1))
    val counts = countPrep.reduceByKey((accumValue,newValue) => accumValue + newValue)
    val sortedCounts = counts.sortBy(kvPair=>kvPair._2, false)
    sortedCounts.saveAsTextFile("file:///Users/amarendra/IdeaProjects/sparkLearn/target/resultWordCount")
  }

}
