// Databricks notebook source
// DBTITLE 1,Airport Task 1
// ID, nameOfAirport, cityName, Country, IcCode, FACOd,latitude, longitude,altitude,timestamp, type, timestamp_country
// /FileStore/tables/airports.csv
val airData = sc.textFile("/FileStore/tables/airports.csv")
val LocatedData = airData.filter( line =>{
  (line.split(",")(6) > "\"40\"") && (line.split(",")(3) contains "Island" )
})
LocatedData.collect()
LocatedData.saveAsTextFile("LocatedData.csv")

// COMMAND ----------

// DBTITLE 1,Airport Task 2
val occurance = airData.filter(line => {
  (line.split(",")(11).contains("Pacific/Port_Moresby")) && ((line.split(",")(8).toInt) % 2 == 0)
})
occurance.count()

// COMMAND ----------

// /FileStore/tables/nasa_july.tsv
// /FileStore/tables/nasa_august.tsv

val julyData = sc.textFile("/FileStore/tables/nasa_july.tsv")
val header= julyData.first
julyData.filter(line => line!=header)
//val hostData = julyData.filter( line => line.split(",")(0))

julyData.collect()

// COMMAND ----------



// COMMAND ----------

// DBTITLE 1,Prime Number
// /FileStore/tables/numberData.csv

val numbers = sc.textFile("/FileStore/tables/numberData.csv")
val header= numbers.first
val headerless = numbers.filter(line=>line!=header)
//headerless.collect()
val intData = headerless.map(line=>line.toInt)
//intData.collect()


def prime(a:Int): Boolean={
  var r=true
  var x=0
  if(a==0 || a==1)
  {
    return false
  }
  else
  {
  for( x <- 2 until a)
  {
    if(a%x==0)
    {
      r=false
    }
  } 
  }
  return r
}
intData.collect()
//numbers.collect()

val primeFiltered = intData.filter(line=>prime(line))
primeFiltered.collect()
val sumData = primeFiltered.sum()
println("Total sum "+sumData)
primeFiltered.count()

// COMMAND ----------

val airData = sc.textFile("/FileStore/tables/airports.csv")
val dataRdd = airData.map(line=>(line.split(",")(3),line.split(",")(11)))
val lowered = dataRdd.map(line=>(line._2.toLowerCase))
lowered.collect()


// COMMAND ----------

val julRdd = sc.textFile("/FileStore/tables/nasa_july-6.tsv")
val augRdd = sc.textFile("/FileStore/tables/nasa_august-6.tsv")
val julHost = julRdd.map(x => x.split("\t")(0))

val augHost = augRdd.map(x => x.split("\t")(0))

val interRdd = julHost.intersection(augHost)


// COMMAND ----------

// DBTITLE 1,Average No of friends for each age
val friendData = sc.textFile("/FileStore/tables/FriendsData.csv")
val removedHeader = friendData.filter(x => !x.contains("Id"))
val ageRdd = removedHeader.map(x => (x.split(",")(2).toInt, (1, x.split(",")(3).toFloat)) )
val reducedRdd = ageRdd.reduceByKey( (x,y) => (x._1 + y._1 , x._2 + y._2))
reducedRdd.take(5)
val avgRdd = reducedRdd.mapValues(data => data._2 / data._1)
avgRdd.collect()

// COMMAND ----------

// DBTITLE 1,Maximum number of friends 
val ageRdd2 = removedHeader.map(x => (x.split(",")(2), x.split(",")(3).toInt) )
val maxRdd = ageRdd2.max()(new Ordering[Tuple2[String, Int]]() {
  override def compare(x: (String, Int), y: (String, Int)): Int =
    Ordering[Int].compare(x._2, y._2)
})
