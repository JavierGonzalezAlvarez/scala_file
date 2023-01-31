package org.jga.files

import java.io.File
import scala.io.Source
import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

//singleton object for an anonymous class
object file {

  def main(args: Array[String]): Unit = {
    //log4j to show errors/warnings
    Logger.getLogger("org").setLevel(Level.ERROR)

    //open a file
    println(getClass)
    println(getClass.getClassLoader.getResource("data/sales.csv"))
    val filename = new File(getClass.getClassLoader.getResource("data/sales.csv").getPath)
    println(filename)
    for (line <- Source.fromFile(filename).getLines()){
      println(line)
    }

    //get the .csv file in a List
    val fileList = Source.fromFile(filename).getLines().toList
    println("list: ", fileList)

    //get the .csv file in an Array
    val fileArray = Source.fromFile(filename).getLines().toArray
    println("total elements: ", fileArray.length)
    println("element 1: ", fileArray(1))

    //get column 1
    for (line <- Source.fromFile(filename).getLines()) {
      val cols = line.split(",").map(_.trim)
      println("column 1: ", cols(0))
    }

    //using spark
    val spark = SparkSession
      .builder
      .appName("SparkCSV")
      .master("local[*]")
      .getOrCreate()

    val df = spark
        .read
        .option("header", false)
        .option("inferSchema", "true")
        .csv(filename.toString)
        .toDF("type", "col1", "col2", "col3", "col4")
    df.printSchema()
    df.show()

    //cast column to the same type, new df is needed
    val df1 = df
      //.withColumn("col1", regexp_replace($"col1", ',', "").cast(DoubleType))  => wrong!
      .withColumn("col1", expr("regexp_replace(col1, ',', '.')").cast(DoubleType))
      .withColumn("col2", expr("regexp_replace(col2, ',', '.')").cast(DoubleType))
      .withColumn("col3", expr("regexp_replace(col3, ',', '.')").cast(DoubleType))
      .withColumn("col4", expr("regexp_replace(col4, ',', '.')").cast(DoubleType))
    df1.printSchema()
    df1.show(truncate = false)

    //some extracted data
    val count = df1.count()
    println(count)
    df1.filter("col1 < 500").show()
    df1.select(sum("col1")).show()
    df1.select(avg("col2")).show()
    df1.select(min("col3")).show()
  }
}
