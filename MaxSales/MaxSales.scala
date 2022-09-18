//1.find maximum number of sale
//2.max sales in particular state (PR,CA,NJ)
//3.max sales in each state
//customer_id,customer_fname,customer_lname,customer_email,customer_pwd,customer_street,customer_city,customer_state,zip_code,number_of_orders,voice_mail_active

import org.apache.spark.SparkContext

object MaxNumberOfSales extends App{
 val sc = new SparkContext("local[*]","MaxSale")

val rdd = sc.textFile("c:/Users/vaibhav/Desktop/SPARK/customer_records.csv")

  val header = rdd.first()
  
  val withoutheader = rdd.filter (x => x!= header)
  
  val splitData = withoutheader.map(x => x.split(","))
  
  val sales1 = splitData.map(x => x(9).toInt).max
  
  //println(sales1)  //MaxSale
  
  val partiularstate2 = splitData.filter(x => x(7) == "PR" || x(7) == "CA" || x(7) == "NJ").map(x => (x(7),x(9).toInt)).max()
  
  //println(partiularstate2) //particular state (PR,CA,NJ)
  
  val maxSaleEachState3 = splitData.map(x => (x(7),x(9).toInt)).reduceByKey(math.max(_,_)).sortByKey()
  maxSaleEachState3.foreach(println)
  


}	