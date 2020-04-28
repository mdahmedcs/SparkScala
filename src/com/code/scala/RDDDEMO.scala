/*
 * 
 * 
 * 
 * */


package com.code.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType


object RDDDEMO{  
    def main(args:Array[String]): Unit={  

  val spark=SparkSession.builder().master("local[4]").appName("rdd demo").getOrCreate()
  
  val sc= spark.sparkContext
  sc.setLogLevel("ERROR")
  
  //Method1: Parallelizing a collection
  val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15))
      
      //  rdd.repartition(2)
      rdd.cache()
      
      
      println("print elements of rdd")
      rdd.collect().foreach(println)
      
      println("map function")
      rdd.map(a=>a*a).collect().foreach(println)
      
      println("filter function")
      rdd.filter(a=>a!=11).collect().foreach(println)
      
      rdd.union(rdd).collect().foreach(println)
      
      println("number of partitions "+rdd.getNumPartitions)
      
      println("First " +rdd.first())
      
      //create rdd from seq
      val rddseq=spark.sparkContext.parallelize(Seq(("Java", 20000), 
  ("Python", 100000), ("Scala", 3000)))
  
  rddseq.collect.foreach(println)
      
      // Method2: Reading from files: Text, CSV, 
      
     val rdd2=sc.textFile("C:\\Users\\ahmed\\Downloads\\subscribers.csv,C:\\Users\\ahmed\\Downloads\\subscribers.csv") //reading from multiple files
     
     rdd2.collect().take(10).foreach(println)
     println("count: "+rdd2.count())
     
var rdd3 = rdd2.map(f=>f.split(","))

  println("Iterate RDD")
  rdd3.take(10).foreach(f=>{
    println("Col1:"+f(0)+",Col2:"+f(1))
  })
       
     
  //word count example
  val text=sc.textFile("E:\\spark_examples\\words.txt")
  
  text.flatMap(x=>x.split(" ")).map(y=>(y,1)).reduceByKey(_+_).sortByKey().collect().foreach(println)   //one line computation for word count
  
  //RDD to DataFrame by manually specifying the schema
  
  //RDD to dataframe using todf method
  
  import spark.implicits._
  val data=Seq(("ahmed", 100), ("mike", 200))
  
  val result=sc.parallelize(data).toDF("name" ,"id")    //creating rdd and converting to DF using toDF method, note it is not necessary to manually mention sc.parallelize, we can instead just use data.toDF("name", "id")
  
  result.printSchema()
  
  result.show();
  
  //RDD to dataframe using spark.createDataFrame method. This approach has two methods
  
  //method1: it takes rdd object as an argument. and chain it with toDF() to specify names to the columns.
  // val dfFromRDD2 = spark.createDataFrame(rdd).toDF(columns:_*)
  //Here, we are using scala operator :_* to explode columns array to comma-separated values.
    
    val sampledata=Seq(("ahmed", 100), ("mike", 200),("john", 300))
    val columns=Seq("name", "id")
    
    val sampledatardd=sc.parallelize(sampledata)
    val resultDF = spark.createDataFrame(sampledatardd).toDF(columns:_*)
    resultDF.show()
    
    // Method2: passing rdd and schema arguments in createDataFrame method
    //syntax: DF= spark.createDataFrame(rdd,schema)
    //To use this first, we need to convert our “rdd” object from RDD[T] to RDD[Row]. To define a schema, we use StructType that takes an array of StructField. And StructField takes column name, data type  nullable/not as arguments.
   //Steps:
   //Create an RDD of Rows from the original RDD;
   //Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
   // Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.

 //creating rdd   
 val sample=Seq(Row("ahmed", 100), Row("mike", 200), Row("john", 300), Row("nick", 400), Row("michael", 500))
 val samplerdd=sc.parallelize(sample)
 
 //creating schema
 val schema = StructType(Array(StructField("name", StringType, nullable=true),StructField("id", IntegerType, nullable=true)))  //schema is StructType of Array of StructFields
  
 //creating df by passing rdd and schema arguments in createDataFrame method
 val DF=spark.createDataFrame(samplerdd,schema)  //here StrucType takes array of StructFields 
 
 DF.show()
 DF.printSchema()
 println(DF.count())
  
    }
} 