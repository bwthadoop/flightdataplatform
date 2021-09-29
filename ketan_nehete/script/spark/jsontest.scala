package dataqualitycheck

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, expr}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.DefaultFormats

import scala.io.Source

object jsontest {



  // convert json to string
  def loadjson(filepath: String): String = {
    val filename = filepath
    val filesource = Source.fromFile(filename)
    var s = ""
    for (line <- filesource.getLines) {
      s = s.concat(line)
    }
    s
  }


  //nullcheck
  def nullcheck(loadjson: String): Array[String] = {
    implicit val formats = DefaultFormats
    val nullcheckcolumn = (parse(loadjson) \ "null_check").extract[String]
    var colname = nullcheckcolumn.split(",")
    colname
  }

  //var s="esal = 'null' or edol ='null'"

  //null expr
  def exprnull(colname: Array[String]): String = {
    var s = ""

    for (i <- 0 until colname.length) {
      if (i != 0) {
        s = s.concat(" or ")
      }
      s = s.concat(colname(i) + " = 'null'").concat(" or " + colname(i) + " is null")
    }

    s

  }


  //**********************************
  //check datatype with json path
  def check(loadjson: String, dataFrame: DataFrame): DataFrame = {

    implicit val formats = DefaultFormats
    val datatypecol = (parse(loadjson) \ "datatype_check").extract[Map[String, String]]

    var c = datatypecol.keys.mkString(",").split(",")
    var d = datatypecol.values.mkString(",").split(",")
    var a = dataFrame
    if (c.length == 1) {
      a = dataFrame.select(col("*"), col(c(0)).cast(d(0)).isNotNull.as("value0")).filter(expr("value0 = 'true'")).select(dataFrame.columns.map(m => col(m)): _*)
    }
    else {
      for (i <- 1 until c.length) {
        var last = i - 1
        var z1 = "value" + (i - 1)
        var z = "value" + i
        var c1 = dataFrame.select(col("*"), col(c(last)).cast(d(last)).isNotNull.as(z1)).filter(expr(z1 + " = 'true'")).select(dataFrame.columns.map(m => col(m)): _*)
        a = c1.select(col("*"), col(c(i)).cast(d(i)).isNotNull.as(z)).filter(expr(z + " = 'true'")).select(dataFrame.columns.map(m => col(m)): _*)
      }
    }
    a
  }


  //create bad record for check datatype with json path
  def checkbad(loadjson: String, dataFrame: DataFrame): DataFrame = {

    implicit val formats = DefaultFormats
    val datatypecol = (parse(loadjson) \ "datatype_check").extract[Map[String, String]]

    var c = datatypecol.keys.mkString(",").split(",")
    var d = datatypecol.values.mkString(",").split(",")
    var a = dataFrame
    if (c.length == 1) {
      a = dataFrame.select(col("*"), col(c(0)).cast(d(0)).isNotNull.as("value0")).filter(expr("value0 = 'false'")).select(dataFrame.columns.map(m => col(m)): _*)
    }
    else {
      for (i <- 1 until c.length) {
        var last = i - 1
        var z1 = "value" + (i - 1)
        var z = "value" + i
        var c1 = dataFrame.select(col("*"), col(c(last)).cast(d(last)).isNotNull.as(z1))
        a = c1.select(col("*"), col(c(i)).cast(d(i)).isNotNull.as(z)).filter(expr(z + " = 'false'")).select(dataFrame.columns.map(m => col(m)): _*)
      }
    }
    a
  }


  //specialchartecher
  def specialcharcheck(loadjson: String): String = {
    implicit val formats = DefaultFormats
    val specialchar = (parse(loadjson) \ "special_character_check").extract[Map[String, String]]
    var colname = specialchar.keys.mkString(",").split(",")
    var spvalue = specialchar.values.mkString(",").split(",")
    var expr1 = exprspecial(colname, spvalue)
    expr1
  }


  //experession builder for specil chr
  def exprspecial(colname: Array[String], spchar: Array[String]): String = {
    var s = ""

    for (i <- 0 until colname.length) {
      if (i != 0) {
        s = s.concat(" or ")
      }
      s = s.concat(colname(i) + " rlike " + "'[" + spchar(i) + "]'")
    }

    s

  }


  //fetch values
  def fetchvalue(loadjson: String, jsonkey: String): Array[String] = {
    implicit val formats = DefaultFormats
    val jsonkey1 = (parse(loadjson) \ jsonkey).extract[String]
    var value = jsonkey1.split(",")
    value
  }





}