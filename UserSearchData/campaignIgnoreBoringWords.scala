//csv contain data of search keyword by user and spend money on words and other contain 
//boringWords file contains boring words.
//Find on Data word how many money has been spend and ignore the boring words
import org.apache.spark.SparkContext
import java.awt.Desktop
import scala.io.Source


object campaignIgnoreBoringWords extends App {
  
  def loadBoaringWords(): Set[String] = {
    var boringWords: Set[String] = Set()
    val lines = Source.fromFile("c:/Users/vaibhav/Desktop/trendytech/week10/boringWords.txt").getLines()
    for(line <- lines){
      boringWords += line
    }
    boringWords
  }

   val sc = new SparkContext("local[*]","UniqueStates")

   val input = sc.textFile("c:/Users/vaibhav/Desktop/trendytech/week10/bigdatacampaigndata.csv")
   
   val nameSet = sc.broadcast(loadBoaringWords)
   
   val mapped_input = input.map(x => (x.split(",")(10).toFloat,x.split(",")(0)))
   
   val words = mapped_input.flatMapValues(x => x.split(" "))
  
   val final_mapped = words.map(x => (x._2,x._1))
   
   val filterrdd = final_mapped.filter(x => !nameSet.value(x._1))
   
   val total = filterrdd.reduceByKey(_+_)
   
   val sorted = total.sortBy(x => x._2,false)
   
   sorted.take(100).foreach(println)
   
}