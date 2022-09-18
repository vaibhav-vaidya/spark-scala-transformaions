import org.apache.spark.SparkContext

object FindUniqueState extends App{
   val sc = new SparkContext("local[*]","UniqueStates")
  
  val rdd = sc.textFile("c:/Users/vaibhav/Desktop/SPARK/scala/spark-scala-transformaion/customer_records.csv")

  val header = rdd.first()
  
  val headerrdd = rdd.filter (x => x!= header)
 
  val States = headerrdd.map(x => (x.split(",")(7)))
  
  val disitinctstate = States.distinct()
  
  disitinctstate.foreach(println)
  
}