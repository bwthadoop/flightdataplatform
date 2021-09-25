//name :- shubham pandey
//sub:- dataquality check



package com.dataquality.check

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

object dataqualitychecks {
  def main(args: Array[String]): Unit = {

    val spark=SparkSession.builder().master("local").appName("spark dataqualitcheck").getOrCreate()


    val df= spark.read.option("header","true").csv("C:\\Users\\shubham\\IdeaProjects\\bwt_spark\\src\\sourcedata\\airports.csv")


    val c=df.rdd

    val validateDF=c.map{row=>row}
   // validateDF.collect().foreach(println)
    //df.printSchema()
   // df.show()



    val originalSchema=df.schema

    val newSchema=originalSchema

    val checkDF=spark.createDataFrame(validateDF,newSchema)
   // checkDF.show()

//    airport_id,city,state,name
    val errorDF = checkDF.filter(checkDF("airport_id").isNull || checkDF("city").isNull || checkDF("state").isNull || checkDF("name").isNull ||  checkDF("airport_id").rlike("[A-Za-z]")  || checkDF("name").rlike("@|!|#") ).toDF()


    println("errordf")
    errorDF.show()
    errorDF.printSchema()

    errorDF.write.csv("C:\\Users\\shubham\\IdeaProjects\\bwt_spark\\src\\sourcedata\\BadDataoutput")


    val errorFreeDF=checkDF.filter(checkDF("airport_id").isNotNull  && checkDF("city").isNotNull && checkDF("state").isNotNull && checkDF("name").isNotNull && checkDF("airport_id").rlike("^[0-9]*$") && checkDF("name").rlike("[A-Za-z ]")  && !checkDF("name").rlike("@|!|#")).toDF()

    val errorFreeDFS=errorFreeDF.withColumn("airport_id",col("airport_id").cast(IntegerType))


    println("errorfreedf")
    errorFreeDFS.show()
    errorFreeDFS.printSchema()

    errorFreeDFS.write.csv("C:\\Users\\shubham\\IdeaProjects\\bwt_spark\\src\\sourcedata\\GoodData")
  }
}

