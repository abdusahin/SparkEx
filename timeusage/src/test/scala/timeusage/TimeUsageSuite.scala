package timeusage

import java.math.MathContext

import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.util.Random
import scala.collection.JavaConverters._
import scala.math.BigDecimal.RoundingMode

@RunWith(classOf[JUnitRunner])
class TimeUsageSuite extends FunSuite with BeforeAndAfterAll with Matchers {


  test("dfSchema returns StructType") {
    val expectedStruct = StructType(Seq(
      StructField("tucaseid", StringType, false),
      StructField("gemetsta", DoubleType, false),
      StructField("gtmetsta", DoubleType, false)
    ))
    val columnNames = List("tucaseid", "gemetsta", "gtmetsta")
    val actualStruct = TimeUsage.dfSchema(columnNames)
    actualStruct shouldBe expectedStruct
  }

  test("read row csv line into a row") {
    val expectedRow = Row("A", 0.001d, 0.002d, 0.003d)
    val testLine = "A,0.001,0.002,0.003".split(",").toList
    val actualRow = TimeUsage.row(testLine)
    actualRow shouldBe expectedRow
  }

  test("build classified columns") {
    val columns = ""
    val primaryNeeds ="t0101,t0102,t0301,t0302,t1101,t1102,t180101,t180102,t180301,t180302"
    val work = "t0501,t0502,t180501,t180502"
    val leisure = "t0201,t0202,t0401,t0402,t0601,t0602,t0701,t0702,t0801,t0802,t0901,t0902,t1001,t1002,t1201,t1202,t1301,t1302,t1401,t1402,t1501,t1502,t1601,t1602,t18100,t18200"

    val testLine = s"tucaseid,$primaryNeeds,$work,$leisure".split(",").toList
    val (actualPrimaryNeeds, actualWork, actualLeisure) = TimeUsage.classifiedColumns(testLine)
    actualPrimaryNeeds shouldBe primaryNeeds.split(",").map(new org.apache.spark.sql.ColumnName(_))
    actualWork shouldBe work.split(",").map(new org.apache.spark.sql.ColumnName(_))
    actualLeisure shouldBe leisure.split(",").map(new org.apache.spark.sql.ColumnName(_))
  }

  test("calculate time usage summary")  {
    val primaryNeeds ="t0101,t0102,t0301,t0302,t1101,t1102,t180101,t180102,t180301,t180302"
    val primaryNeedsColumn = primaryNeeds.split(",").map(new org.apache.spark.sql.ColumnName(_)).toList
    val work = "t0501,t0502,t180501,t180502"
    val workColumn = work.split(",").map(new org.apache.spark.sql.ColumnName(_)).toList
    val leisure = "t0201,t0202,t0401,t0402,t0601,t0602,t0701,t0702,t0801,t0802,t0901,t0902,t1001,t1002,t1201,t1202,t1301,t1302,t1401,t1402,t1501,t1502,t1601,t1602,t18100,t18200"
    val leisureColumn = leisure.split(",").map(new org.apache.spark.sql.ColumnName(_)).toList
    val interestedCols = s"$primaryNeeds,$work,$leisure"
    val columns = s"tucaseid,telfs,tesex,teage,$interestedCols".split(",").toList

    val row1 = List("A", "1", "1", "15") ++ primaryNeeds.split(",").map(_ => "10") ++
      work.split(",").map(_ => "20") ++ leisure.split(",").map(_ => "30")

    val schema = TimeUsage.dfSchema(columns)
    val row = TimeUsage.row(row1)

    val dataFrameTest  = spark.createDataFrame(List(row).asJava, schema)

    val dataFrameSummary = TimeUsage.timeUsageSummary(primaryNeedsColumn, workColumn, leisureColumn, dataFrameTest)
    dataFrameSummary.toJavaRDD.rdd.collect().head.getAs[String]("working") shouldBe "working"
    dataFrameSummary.toJavaRDD.rdd.collect().head.getAs[String]("sex") shouldBe "male"
    dataFrameSummary.toJavaRDD.rdd.collect().head.getAs[String]("age") shouldBe "young"
    val scale = BigDecimal(dataFrameSummary.toJavaRDD.rdd.collect().head.getAs[Double]("primaryNeeds")).setScale(3, RoundingMode.HALF_UP)
    scale shouldBe BigDecimal(1.667)
    BigDecimal(dataFrameSummary.toJavaRDD.rdd.collect().head.getAs[Double]("work")).setScale(3, RoundingMode.HALF_UP) shouldBe BigDecimal(1.333)
    BigDecimal(dataFrameSummary.toJavaRDD.rdd.collect().head.getAs[Double]("other")).setScale(3, RoundingMode.HALF_UP) shouldBe BigDecimal(13.0)

  }

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Time Usage")
      .config("spark.master", "local")
      .getOrCreate()


  override protected def afterAll(): Unit = {
    spark.stop()
  }
}
