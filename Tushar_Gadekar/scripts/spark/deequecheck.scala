package Classpractice

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

object DQfinalcheck {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("Deeque check for firstflightdata1").getOrCreate()
    // -Xmx512m
    spark.sparkContext.setLogLevel("ERROR")


    if (args.length != 3) {
      println("check the number of argument")
      System.exit(1)
    }

    var jsonpath = args(0)
    var inputfile = args(1)
    var outputpath = args(2)

    // convert json to string

    val source = scala.io.Source.fromFile(jsonpath)
    val input = try source.mkString finally source.close()
    val json1 = ujson.read(input)
    var jsonfile1 = json1
    var json = jsonfile1.toString()
    println(jsonfile1)

    println("*************CSV DataFrame****************************")

    var testdf1 = spark.read.csv(inputfile)
    testdf1.show()
    testdf1.printSchema()

    println("**************Code-For-Header list***************************")
    //header_list check
    val header = jsonfile1("header_list")
    var header_list = header.toString().replace("\"", "").split(',')
    println(header)
    println("*****************************************")

    if (json.contains("header_list")) {
      testdf1 = spark.read.csv(inputfile).toDF(header_list: _*)
    }
    else if ((json, "header") == "true") {
      testdf1 = spark.read.option("header", "true").csv(inputfile)
    }
    else {
      testdf1 = spark.read.csv(inputfile)
    }
    testdf1.show()
    var gooddf = testdf1.coalesce(1)
    var baddf = spark.emptyDataFrame.coalesce(1)


    println("*****************************************")
    val null1 = jsonfile1("null_check")
    var null_check = null1.toString().replace("\"", "").split(",")
    println(null1)
    // println(null_check)

    println("*****************************************")
    //nullcheck

    if (json.contains("null_check")) {
      var s = ""
      for (i <- 0 until null_check.length) {
        if (i != 0) {
          s = s.concat(" or ")
        }
        s = s.concat(null_check(i) + " = 'null'").concat(" or " + null_check(i) + " is null")
        println(i)
      }

      println(s)

      //fetching null check columns
      //creating expr for nullcheck
      gooddf = testdf1.where(!expr(s))
      baddf = testdf1.where(expr(s))
    }

    println("nullcheck for good & bad file")
    println("**************good dataFrame***************************")
    gooddf.show()
    println("**************Bad dataFrame***************************")
    baddf.show()
    println("*****************************************")

    //spcheck
    println("*****************************************")
    val special_char = jsonfile1("special_character_check")
    var special_char_check = special_char.toString()
    //.replace("\"", "").split(",")
    println(special_char)
    println(special_char_check)

    implicit val formats = DefaultFormats
    val specialchar = (parse(json) \ "special_character_check").extract[Map[String, String]]
    println(specialchar)

    var colname = specialchar.keys.mkString(",").split(",")
    var spvalue = specialchar.values.mkString(",").split(",")

    //spcheck

    if (json.contains("special_character_check")) {
      var s = ""

      for (i <- 0 until colname.length) {
        if (i != 0) {
          s = s.concat(" or ")
        }
        s = s.concat(colname(i) + " rlike " + "'[" + spvalue(i) + "]'")
      }
      println(s)


      val exprsp = jsonfile1
      var spgood = gooddf.where(!expr(s))

      if (baddf.rdd.isEmpty()) {
        baddf = gooddf.where(expr(s))
      }
      else {
        baddf = baddf.union(gooddf.where(expr(s)))
      }
      gooddf = spgood
    }

    println("spcheck good or bad")
    gooddf.show()
    baddf.show()

    println("*******************Datatype check**********************")

    //datatype check

    val data_type = jsonfile1("datatype_check")
    var data_type_check = data_type.toString()
    //.replace("\"", "").split(",")
    println(data_type)
    println(data_type_check)

    // implicit val formats = DefaultFormats
    val datatypecol = (parse(json) \ "datatype_check").extract[Map[String, String]]

    println(datatypecol)

    var key = datatypecol.keys.mkString(",").split(",")
    var value = datatypecol.values.mkString(",").split(",")
    println(key)
    println(value)

    if (json.contains("data_type_check")) {
      var a = gooddf
      if (key.length == 1) {
        a = gooddf.select(col("*"), col(key(0)).cast(value(0)).isNotNull.as("value0")).filter(expr("value0 = 'true'")).select(gooddf.columns.map(m => col(m)): _*)
      }
      else {
        for (i <- 1 until key.length) {
          var last = i - 1
          var z1 = "value" + (i - 1)
          var z = "value" + i
          var c1 = gooddf.select(col("*"), col(key(last)).cast(value(last)).isNotNull.as(z1)).filter(expr(z1 + " = 'true'")).select(gooddf.columns.map(m => col(m)): _*)
          a = c1.select(col("*"), col(key(i)).cast(value(i)).isNotNull.as(z)).filter(expr(z + " = 'true'")).select(gooddf.columns.map(m => col(m)): _*)
        }
      }

    }
    gooddf.show()


    if (json.contains("data_type_check")) {

      var a = baddf
      if (key.length == 1) {
        a = baddf.select(col("*"), col(key(0)).cast(key(0)).isNotNull.as("value0")).filter(expr("value0 = 'false'")).select(baddf.columns.map(m => col(m)): _*)
      }
      else {
        for (i <- 1 until key.length) {
          var last = i - 1
          var z1 = "value" + (i - 1)
          var z = "value" + i
          var c1 = baddf.select(col("*"), col(key(last)).cast(value(last)).isNotNull.as(z1))
          a = c1.select(col("*"), col(key(i)).cast(value(i)).isNotNull.as(z)).filter(expr(z + " = 'false'")).select(baddf.columns.map(m => col(m)): _*)
        }
      }

    }
    baddf.show()

    gooddf.coalesce(1).write.format("csv").mode("overwrite").save(outputpath + "\\goodrecrd")
    baddf.coalesce(1).write.format("csv").mode("overwrite").save(outputpath + "\\badrecord")

  }

}

