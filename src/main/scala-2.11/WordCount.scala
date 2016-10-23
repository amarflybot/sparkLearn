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
    val counts = countPrep.map(word => (word, 1)).reduceByKey{case (x,y) => x + y}
    counts.saveAsTextFile("file:///Users/amarendra/IdeaProjects/sparkLearn/target/resultWordCount")
  }

}
