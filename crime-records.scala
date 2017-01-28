case class CrimeRecord(date:String,Type:String,desc:String,location:String,arrest:String,domestic:String,district:Int,year:Int)

def parseRec(log:String):CrimeRecord = {
    val res = log.split(",")
    CrimeRecord(res(2),res(5),res(6),res(7),res(8),res(9),res(11).toInt,res(17).toInt)		
}

val recLine = sc.textFile("/data/Crimes_-_2001_to_present.csv") #paste the location where the file is kept
val header = recLine.first()
val data = recLine.filter(row => row != header)
val accessRec = data.map(parseRec)
val accessDF = accessRec.toDF()
accessDF.createOrReplaceTempView("crime")
val crimes = spark.sql("select * from crime")
crimes.createOrReplaceTempView("crimeRecords")
spark.sql("cache TABLE crimeRecords")


# Number of crimes reported under each crime type
spark.sql("select Type, count(*) as total from CrimeRecords group by Type order by total desc").show

# Month in which the crime rate is high
spark.sql("select substr(date,1,2) as month, count(*) as total from crimeRecords group by month order by total desc LIMIT 1").show

# Number of theft related arrests that happened in each district
spark.sql("select district,count(*) as arrests from CrimeRecords where Type='THEFT' and arrest='true' group by district order by arrests desc").show

