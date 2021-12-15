package sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import java.util.Date

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Aggregator

import scala.Console.println
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.xml.XML.encoding

object spark_try {
  def main(args: Array[String]): Unit = {
    //统计程序运行时间
    var start_time = new Date().getTime
    //创造SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("spark://10.102.65.234:7077").setAppName("SparkSQL")
//    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("SparkSQL")
//    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate() //对象
//    val spark:SparkSession = SparkSession.builder()
//      .appName("SparkSQL")
//      .master("local[*]")
//      .getOrCreate()

    // 使用createDataFrame1
    val FileRDD = spark.sparkContext.textFile("datas/txt_output5.txt")
      .map(x => x.split(" "))
      .map(x => Row(x(0), x(1), x(2).toInt, x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10).toLong))
    //set schema structure
    val schema = StructType(
      Seq(
        StructField("name", StringType, true),
        StructField("sex", StringType, true),
        StructField("age", IntegerType, true),
        StructField("bir", StringType, true),
        StructField("id", StringType, true),
        StructField("edu", StringType, true),
        StructField("tel", StringType, true),
        StructField("loc", StringType, true),
        StructField("sid", StringType, true),
        StructField("anc", StringType, true),
        StructField("inc", LongType, true)
      )
    )
    val DF = spark.createDataFrame(FileRDD, schema)
    //    val DF2 = spark.createDataFrame(FileRDD,schema)
    //    val DF3 = spark.createDataFrame(FileRDD,schema)
    //    val DF4 = spark.createDataFrame(FileRDD,schema)
    DF.show() //展示所有

    println("--------------------------------------------")
    println("数据库存储中")
    val url = "jdbc:mysql://10.102.65.234:3306/textcluster?useSSL=false&serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8"
    val table = "user4" //student表可以不存在，但是url中指定的数据库要存在
    val prop = new java.util.Properties
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")
    DF.write.mode("append").jdbc(url, table, prop)
    println("--------------------------------------------")
    println("数据库存储完毕")

    //1.统计收入最高的100个人(展示其name，id，income)；2.统计人均收入
    DF.createOrReplaceTempView("income")
    println("--------------------------------------------")
    println("1.统计收入最高的100个人(展示其name，id，income)")
    spark.sql("SELECT name,id,inc FROM income order by inc desc limit 10000").show()
    println("--------------------------------------------")
    println("2.统计人均收入")
    spark.sql("SELECT avg(inc) FROM income").show()

    //2.统计人口性别构成
    DF.createOrReplaceTempView("sex")
    println("--------------------------------------------")
    println("3.统计人口性别构成")
    spark.sql("select count(*) as male from sex where sex ='male' ").show()
    spark.sql("select count(*) as female from sex where sex ='female' ").show()

    //3.统计城镇人口比重
    DF.createOrReplaceTempView("loc")
    println("--------------------------------------------")
    println("4.统计城镇人口比重")
    spark.sql("select count(*) as towns from loc where sid ='towns' ").show()
    spark.sql("select count(*) as village from loc where sid ='village' ").show()

    //4.统计姓Wang的收入>500000 的人数
    DF.createOrReplaceTempView("wang")
    println("--------------------------------------------")
    println("5.统计姓Wang的人数")
    spark.sql("select count(*) as numbers from wang where name like 'Wang%' and inc > 5000000 ").show()

//    //5.统计年龄的平均值
//    DF.createOrReplaceTempView("user")
//    println("--------------------------------------------")
//    println("6.统计年龄的平均值")
//    spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))
//    spark.sql("select ageAvg(age) from user").show()

    spark.close()
    //创造SparkSQL的运行环境
//    val spark2 = SparkSession.builder().config(sparkConf).getOrCreate() //对象

//    println("--------------------------------------------")
//    println("7.统计前十位同名的人数")
//    //词频统计
//    val FileRDD1 = spark2.sparkContext.textFile("datas/txt_output5.txt")
//      .map(x => x.split(" "))
//      .map(x => Row(x(0)))
//    //set schema structure
//    val schema1 = StructType(
//      Seq(
//        StructField("name", StringType, true)
//      )
//    )
//    val DF1 = spark2.createDataFrame(FileRDD1, schema1)
//    val rowRDD: RDD[Row] = DF1.rdd
//    val sourceRdd = rowRDD.map(_.mkString(",")).map(x => (x, 1)).reduceByKey((a, b) => (a + b)).sortBy(_._2, false).collect
//    val top10 = sourceRdd.take(10)
//    top10.foreach(println)
//    spark2.close()
//
    var end_time = new Date().getTime
    println("程序共执行了：" + (end_time - start_time) / 1000 + "s")
//  }


//  case class Buff(var total: Long, var count: Long)
//
//  class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
//    //缓冲区的初始化
//    override def zero: Buff = {
//      Buff(0L, 0L)
//    }
//
//    //根据输入的数据更新缓冲区的数据
//    override def reduce(b: Buff, a: Long): Buff = {
//      b.total = b.total + a
//      b.count = b.count + 1
//      b //返回
//    }
//
//    //合并缓冲区
//    override def merge(b1: Buff, b2: Buff): Buff = {
//      b1.total = b1.total + b2.total
//      b1.count = b1.count + b2.count
//      b1
//    }
//
//    //计算结果
//    override def finish(reduction: Buff): Long = {
//      reduction.total / reduction.count
//    }
//
//    //缓冲区编码操作
//    override def bufferEncoder: Encoder[Buff] = Encoders.product
//
//    //缓冲区输出编码操作
//    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
//  }

}}
