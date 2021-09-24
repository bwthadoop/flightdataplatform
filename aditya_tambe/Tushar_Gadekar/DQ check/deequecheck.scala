package Classpractice
import scala.io.Source

object testdqcheck {

  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder().master("local").appName("Deeque check for firstflightdata").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.parquet.fs.optimized.committer.optimization-enabled", true).getOrCreate()
    // -Xmx512m
    spark.sparkContext.setLogLevel("ERROR")

    println("*********************************")
        val newSchema1: StructType = DataType.fromJson(
      """{
        |  "type": "struct",
        |  "fields": [
        |    {
        |      "name": "airport_id",
        |      "type": "integer",
        |       "nullable":true,
        |      "metadata": {}
        |
        |    },
        |    {
        |      "name": "city",
        |      "type": "string",
        |        "nullable":true,
        |      "metadata": {}
        |
        |    },
        |    {
        |      "name": "state",
        |      "type": "string",
        |       "nullable":true,
        |      "metadata": {}
        |
        |    },
        |    {
        |      "name": "name",
        |      "type": "string",
        |      "nullable":true,
        |      "metadata": {}
        |
        |    }
        |  ]
        |}""".stripMargin).asInstanceOf[StructType]

#//  "pattern": "^[A-Za-z0-9\s]+$"
# //  "containsNull":false
#    // "nullable":true,
    println(newSchema1)
    println("*****************************************************")

    println("*****************************************************")
#    //in this case Spark should ignore the whole corrupted records.//

    val airprtdf = spark.read.format("csv").option("multiline", "true")
      .option("delimiter",",")
      .option("header", "true")
    .option("nullValue", "null")
   .option("mode", "DROPMALFORMED")
//    //  .option("mode", "FAILFAST")
      .schema(newSchema1)
      .load("E:\\Hadoop & Java lecture\\Hadoop\\Hadoop temp\\project_2 temp\\airports.csv")

   airprtdf.show()
    airprtdf.printSchema()

    println("*****************************************************")
   airprtdf.na.fill("null").show()
    println("*****************************************************")
     airprtdf.na.fill(0).show(false)
    println("*****************************************************")

#    // Filter out row having errors
    val errorDf = airprtdf.filter(col("airport_id").isNotNull && col("city").isNotNull && col("state").isNotNull && col("name").isNotNull)

#    // Filter our row having no errors
    val errorFreeDf = airprtdf.filter(col("airport_id").isNull || col("city").isNull || col("state").isNull || col("name").isNull)

    errorDf.show()
    println("*****************************************************")
    errorFreeDf.show()

    println("*****************************************************")
        airprtdf.coalesce(1).write.mode("overwrite").csv("E:\\Hadoop & Java lecture\\Hadoop\\Hadoop temp\\project_2 temp\\deeque check output\\")


  }
}
