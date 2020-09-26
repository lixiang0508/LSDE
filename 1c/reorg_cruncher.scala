import org.apache.spark.sql.functions.{asc, col, count, dayofmonth, desc, first, month, collect_list, min}
//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SaveMode,SQLContext}

def reorg(datadir :String) 
{

  // load knows & person & interest
   val t0 = System.nanoTime()

  val knows    = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/knows.*csv.*")
  val person   = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/person.*csv.*")
  val interest = spark.read.format("csv").option("header", "true").option("delimiter", "|").option("inferschema", "true").
                       load(datadir + "/interest.*csv.*")
  // remove different location
  val knows_loc = knows.join(person.select("personId", "locatedIn"), "personId").join(person.select($"personId".alias("friendId"), $"locatedIn".alias("friendLocatedIn")), "friendId")
  val knows_del_loc = knows_loc.filter($"friendLocatedIn" === $"locatedIn").select("personId", "friendId")
  // remove unmutual friends
  val knows_del_unmutual = knows_del_loc.join(knows_del_loc.select($"personId".alias("friendId"), $"friendId".alias("p")), "friendId").filter($"p" === $"personId").select("personId", "friendId")
  val knows_res = knows_del_unmutual.join(person.select("personId", "birthday").withColumn("bday", month($"birthday")*100 + dayofmonth($"birthday")), "personId").sort("bday", "personId")
  val person_res = knows_res.groupBy("personId").agg(first("bday").alias("bday"), collect_list("friendId").alias("friendId"))

  // only keep person with mutual friends
  val interest_res = person_res.join(interest, "personId").sort("interest").withColumn("aId", $"interest")

  interest_res.repartition($"interest").write.mode(SaveMode.Overwrite).option("compression", "snappy").partitionBy("interest").parquet(datadir+"/interest_res.parquet")

   val t1 = System.nanoTime()

  //println("reorg time: " + (t1 - t0)/1000000 + "ms")
}


def cruncher(datadir :String, a1 :Int, a2 :Int, a3 :Int, a4 :Int, lo :Int, hi :Int):org.apache.spark.sql.DataFrame =
{          
  val fileList = List(a1,a2,a3,a4).map(x => datadir + "/interest_res.parquet/interest=" + x)
  val interest = spark.read.parquet(fileList: _*)
  val a1List = spark.read.parquet(datadir + "/interest_res.parquet/interest=" + a1).select($"personId".alias("f"), $"aId")
  
  val res = interest.filter($"bday" >= lo && $"bday" <= hi)
                    .withColumn("noFan", $"aId" =!= a1)
                    .groupBy("personId")
                    .agg(count("personId") as "scores", first("friendId") as "friendId", min("noFan") as "noFan")
                    .filter($"noFan").select("scores", "personId", "friendId")
                    .flatMap (f => f.getSeq[Long](2).map((f.getLong(0), f.getLong(1), _)))
                    .select($"_1".alias("scores"), $"_2".alias("p"), $"_3".alias("f"))
                    .join(a1List, "f").select("p", "f", "scores")
                    .orderBy(desc("scores"), asc("p"), asc("f"))
  
  res.show(1000)
  return res

}
