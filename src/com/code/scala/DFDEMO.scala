package com.code.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.min
import org.apache.spark.sql.functions.max
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.functions._
import java.sql.SQLException;


object DFDEMO {
  
  def main(args:Array[String]): Unit={
  
 
  
  val spark:SparkSession = SparkSession.builder().master("local[4]").appName("demo").getOrCreate()
  val sc = spark.sparkContext
  
//  import spark.sqlContext.implicits._
  
  sc.setLogLevel("ERROR")
  
 val sample=Seq(Row("ahmed", 100,5000), Row("mike", 200,5000), Row("john", 300,15000), Row("nick", 400,10000), Row("michael", 500,15000))
 val samplerdd=sc.parallelize(sample)
 
 //creating schema
 val fields = List(StructField("name", StringType, nullable=true),StructField("id", IntegerType, nullable=true),StructField("salary", IntegerType, nullable=true))  //Notice Struct Field has three parameters: name, type and nullable
 val schema = StructType(fields)
 
 //creating df by passing rdd and schema arguments in createDataFrame method
 var DF=spark.createDataFrame(samplerdd,schema)  //here StrucType takes array of StructFields 
 
 //withColumn 
 val DF2=DF.withColumn("multiplication", col("salary")*100)
 DF2.show()
 
 //withColumn using when otherwise
  val DFWhen = DF.withColumn("dept", when(col("id")<=200, "IT").when(col("id")>200 and col("id")<500, "Network").otherwise("other"))
  DFWhen.show();
  
  
  //filter/where
  val DFFilter = DF.filter(col("id")<300)
  DFFilter.show()
 
  //min, max, sum, avg
 DF.select(min(col("salary"))).show()
 DF.select(max(col("salary"))).show()
 DF.select(sum(col("salary"))).show()
 DF.select(avg(col("salary"))).show()
    
 //groupBy
 DF.groupBy("id").sum("salary").show()
 
 //distinct
 DF.select(col("salary")).distinct().show()

 //sort
 
 DF.sort(col("salary").desc).show()
 DF.sort(col("salary").asc).show()
 
 // joinS
 val bonusDF=DF.withColumn("bonus", col("salary")/4).select(col("id"),col("bonus"))  
 
//inner join
 DF.join(bonusDF,DF("id") ===  bonusDF("id"),"inner").show(false)
 
 //left join
 DF.join(bonusDF,DF("id") ===  bonusDF("id"),"left").show(false)
 
 //union
 println("applying union")
 
 val DFunion=DF  //union requires both data frames to have same structure
 DF.union(DFunion).show(false)
 
 println("applying union selecting distinct records")
 DF.union(DFunion).distinct().show(false)
 
 
 
 //Reading from csv using infer schema, for csv file infer schema is false by default
 
   val dfcsv=spark.read.options(Map("inferschema"->"true", "header"->"true")).csv("E:\\spark_examples\\subscribers.csv")
  
     dfcsv.show()
     dfcsv.printSchema()
  
 //Reading from csv by programatically specifying schema
  
   val schemacsv = StructType(Array(StructField("custnum", IntegerType, nullable=true),StructField("ordernum", IntegerType, nullable=true),StructField("email", StringType, nullable=true),StructField("fname", StringType, nullable=true),
       StructField("lname", StringType, nullable=true),StructField("title", StringType, nullable=true),StructField("company", StringType, nullable=true),StructField("address", StringType, nullable=true),StructField("city", StringType, nullable=true),
       StructField("state", StringType, nullable=true),StructField("zip", StringType, nullable=true),StructField("country", StringType, nullable=true),StructField("phone", StringType, nullable=true),StructField("brand", StringType, nullable=true)))  
       
   val dfsamplecsv=spark.read.format("csv")
       .option("header","true")
       .schema(schemacsv)
       .load("E:\\spark_examples\\subscribers2.csv")
   
     dfsamplecsv.show(100,false)
     dfsamplecsv.printSchema()
     
     //createOrReplaceTempView: The lifetime of this temporary view is tied to the [[SparkSession]] that was used to create this Dataset.
     //createGlobalTempView: The lifetime of this temporary view is tied to this Spark application.
     
     println("createorReplaceTempView")
     dfsamplecsv.createOrReplaceTempView("v1") 
     
    val dftempview = spark.sql("select custnum,ordernum,email,state from v1 where lower(state)='tx' limit 25")
     
     dftempview.show();
     
   
  // Redaing from JSON. for JSON infershema is true by default
     println("reading from json using infeschema, by default true")
     
     val dfjson=spark.read.format("json").load("E:\\spark_examples\\subscribers.json")
     
     dfjson.show(10,false)
     dfjson.printSchema()
          
 //    println("writing data to json file")
 //    dfjson.write.format("json").save("E:\\spark_examples\\subscribers_json.json") //  4 savemodes: overwrite, append, ignore, default:errorifexists
     
  // Reading from JSON by programatically specifying schema
     
     val schemajson = StructType(Array(StructField("city", StringType, nullable=true),StructField("recnum", LongType, nullable=true),StructField("state", StringType, nullable=true),StructField("type", StringType, nullable=true),
     StructField("zipcode", LongType, nullable=true)))
       
      val dfjsonsamplejson = spark.read.format("json")
                             .schema(schemajson)
                             .load("E:\\spark_examples\\subscribers.json")
                             
      dfjsonsamplejson.show()
      dfjsonsamplejson.printSchema()
       
      
/*  //Reading from jdbc: mysql
   
   println("reading from MySQL")   
   val DFMySQL =   spark.read.format("jdbc")
  .option("url", "jdbc:mysql://localhost:3306/test")
  .option("dbtable", "test.subscriber")
  .option("user", "ahmed")
  .option("password", "123456")
  .load()
  
  DFMySQL.show()
  */
 
  
    
  
  
}

}