//csv contain data of search keyword by user and spend money on words
//Find on Data word how many money has been spend
import org.apache.spark.SparkContext
import java.awt.Desktop


object Campaign extends App {

   val sc = new SparkContext("local[*]","UniqueStates")

   val input = sc.textFile("c:/Users/vaibhav/Desktop/trendytech/week10/bigdatacampaigndata.csv")
   
   val mapped_input = input.map(x => (x.split(",")(10).toFloat,x.split(",")(0)))
   
   val words = mapped_input.flatMapValues(x => x.split(" "))
  
   val final_mapped = words.map(x => (x._2,x._1))
   
   val total = final_mapped.reduceByKey(_+_)
   
   val sorted = total.sortBy(x => x._2,false)
   
   sorted.take(20).foreach(println)
   
   
   
   
   
   
   
   
   
   
}