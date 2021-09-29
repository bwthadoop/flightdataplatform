
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, count, expr}
import org.apache.spark.sql.types.{DateType, IntegerType, LongType, StringType, StructField, StructType}
import jsontest._
import org.apache.spark.sql.expressions.Window

object DQCheck {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local").appName("dqcheck").getOrCreate()
    spark.sparkContext.setLogLevel("Error")

    if (args.length != 3) {
      println("check the number of argument")
      System.exit(1)
    }
    var jsonpath = args(1)
    var inputfile = args(0)
    var outputpath = args(2)
    var jsonfile = jsontest.loadjson(jsonpath)


    //creatting dataframe
    var df = spark.read.csv(inputfile)

    if (jsonfile.contains("header_list")) {
      df = spark.read.csv(inputfile).toDF(fetchvalue(jsonfile, "header_list"): _*)
    }
    else if (fetchvalue(jsonfile, "header")(0) == "true") {
      df = spark.read.option("header", true).csv(inputfile)
    }
    else {
      df = spark.read.csv(inputfile)
    }
    df.show()
    var gooddf = df.coalesce(1)
    var baddf = spark.emptyDataFrame.coalesce(1)


    //nullcheck
    if (jsonfile.contains("null_check")) {
      //fetching null check columns
      val checknull = nullcheck(jsonfile)

      //creating expr for nullcheck
      var exprfornull = jsontest.exprnull(checknull)
      gooddf = df.where(!expr(exprfornull))
      baddf = df.where(expr(exprfornull))
    }

    println("nullcheck good bad")
    gooddf.show()
    baddf.show()


    //spcheck

    if (jsonfile.contains("special_character_check")) {
      var exprsp = jsontest.specialcharcheck(jsonfile)
      var spgood = gooddf.where(!expr(exprsp))

      if (baddf.rdd.isEmpty()) {
        baddf=gooddf.where(expr(exprsp))

      }
      else {
        baddf = baddf.union(gooddf.where(expr(exprsp)))
      }
      gooddf=spgood
    }

    println("spcheck good or bad")
    gooddf.show()
    baddf.show()

    //datatype check
    if (jsonfile.contains("datatype_check")) {

      var good_data = check(jsonfile,gooddf)

      if (baddf.rdd.isEmpty()) {
        baddf=checkbad(jsonfile,gooddf)

      }
      else {
        baddf = baddf.union(checkbad(jsonfile,gooddf))
      }
      gooddf=good_data
    }

    println("datatype_check ")
    gooddf.show()
    baddf.show()
    checkbad(jsonfile,df).show()


    gooddf.coalesce(1).write.format("csv").mode("overwrite").save(outputpath + "\\goodrecrd")
    baddf.coalesce(1).write.format("csv").mode("overwrite").save(outputpath + "\\badrecord")

  }

}
