import java.sql.Timestamp

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming.OutputMode
import org.scalatest.FunSuite


class StreamJoinTest extends FunSuite with DataFrameSuiteBase {
  import spark.implicits._

  protected override val enableHiveSupport = false

  def ts(year: Int = 0, month: Int = 1, day: Int = 1): Timestamp =
    Timestamp.valueOf(f"2$year%03d-$month%02d-$day%02d 00:00:00")

  test("eventTimeJoinStaticToStatic") {
    val fact = sc.parallelize(Seq(
      (ts(1), "A"),
      (ts(2), "B"),
      (ts(3), "A")
    )).toDF("eventTime", "city")
    fact.createOrReplaceTempView("fact")

    val dim = sc.parallelize(Seq(
      (ts(0), "A", 1),
      (ts(0), "B", 2),
      (ts(1), "B", 3),
      (ts(2), "A", 4),
      (ts(4), "A", 5)
    )).toDF("eventTime", "city", "population")
    dim.createOrReplaceTempView("dim")

    val result = spark.sql("""
      SELECT fact.eventTime, fact.city, dim.population
      FROM fact
      LEFT JOIN dim
        ON fact.city = dim.city
          AND dim.eventTime <= fact.eventTime
      LEFT JOIN dim as dim2
        ON fact.city = dim2.city
          AND dim2.eventTime <= fact.eventTime
          AND dim2.eventTime > dim.eventTime
      WHERE dim2.eventTime IS NULL
    """).orderBy("eventTime")

    /* Before WHERE d2.eventTime IS NULL
    +-------------------+----+----------+-------------------+-------------------+
    |          eventTime|city|population|                 d1|                 d2|
    +-------------------+----+----------+-------------------+-------------------+
    |2001-01-01 00:00:00|   A|         1|2000-01-01 00:00:00|               null|
    |2002-01-01 00:00:00|   B|         2|2000-01-01 00:00:00|2001-01-01 00:00:00|
    |2002-01-01 00:00:00|   B|         3|2001-01-01 00:00:00|               null|
    |2003-01-01 00:00:00|   A|         1|2000-01-01 00:00:00|2002-01-01 00:00:00|
    |2003-01-01 00:00:00|   A|         4|2002-01-01 00:00:00|               null|
    +-------------------+----+----------+-------------------+-------------------+
    */

    val expected = sc.parallelize(Seq(
      (ts(1), "A", Some(1)),
      (ts(2), "B", Some(3)),
      (ts(3), "A", Some(4))
    )).toDF("eventTime", "city", "population")

    assertDataFrameEquals(expected, result)
  }

  test("eventTimeJoinStreamToStatic") {
    val factMem = MemoryStream[String](
      1, spark.sqlContext)(Encoders.STRING)
    val fact = factMem.toDF()
      .selectExpr(
        "cast(split(value, ',')[0] as timestamp) as eventTime",
        "split(value, ',')[1] as city"
      )
    fact.createOrReplaceTempView("fact")

    def addFact(t: Timestamp, city: String): Unit = {
      factMem.addData(Seq(t.toString, city).mkString(","))
    }

    val dim = sc.parallelize(Seq(
      (ts(0), "A", 1),
      (ts(0), "B", 2),
      (ts(1), "B", 3),
      (ts(2), "A", 4),
      (ts(4), "A", 5)
    )).toDF("eventTime", "city", "population")
    dim.createOrReplaceTempView("dim")

    val transform = spark.sql("""
      SELECT fact.eventTime, fact.city, dim.population
      FROM fact
      LEFT JOIN dim
        ON fact.city = dim.city
          AND dim.eventTime <= fact.eventTime
      LEFT JOIN dim as dim2
        ON fact.city = dim2.city
          AND dim2.eventTime <= fact.eventTime
          AND dim2.eventTime > dim.eventTime
      WHERE dim2.eventTime IS NULL
    """)

    val query = transform.writeStream
      .format("memory")
      .queryName("result")
      .outputMode(OutputMode.Update())
      .start

    val result = spark.sql("SELECT * FROM result")

    query.processAllAvailable()

    addFact(ts(1), "A")
    query.processAllAvailable()

    addFact(ts(2), "B")
    query.processAllAvailable()

    addFact(ts(3), "A")
    query.processAllAvailable()

    query.processAllAvailable()

    val expected = sc.parallelize(Seq(
      (ts(1), "A", Some(1)),
      (ts(2), "B", Some(3)),
      (ts(3), "A", Some(4))
    )).toDF("eventTime", "city", "population")

    assertDataFrameEquals(expected, result)
  }

  ignore("eventTimeJoinStreamToStream") {
    val factMem = MemoryStream[String](
      1, spark.sqlContext)(Encoders.STRING)
    val fact = factMem.toDF()
        .selectExpr(
          "cast(split(value, ',')[0] as timestamp) as eventTime",
          "split(value, ',')[1] as city"
        )
        .withWatermark("eventTime", "1 minute")
    fact.createOrReplaceTempView("fact")

    def addFact(t: Timestamp, city: String): Unit = {
      factMem.addData(Seq(t.toString, city).mkString(","))
    }

    val dimMem = MemoryStream[String](
      1, spark.sqlContext)(Encoders.STRING)
    val dim = dimMem.toDF()
      .selectExpr(
        "cast(split(value, ',')[0] as timestamp) as eventTime",
        "split(value, ',')[1] as city",
        "cast(split(value, ',')[1] as int) as population"
      )
      .withWatermark("eventTime", "1 minute")
    dim.createOrReplaceTempView("dim")

    def addDim(t: Timestamp, city: String, population: Int): Unit = {
      dimMem.addData(Seq(t.toString, city, population.toString).mkString(","))
    }

    val transform = spark.sql("""
      SELECT fact.eventTime, fact.city, dim.population
      FROM fact
      LEFT JOIN dim
        ON fact.city = dim.city
          AND dim.eventTime <= fact.eventTime
      LEFT JOIN dim as dim2
        ON fact.city = dim2.city
          AND dim2.eventTime <= fact.eventTime
          AND dim2.eventTime > dim.eventTime
      WHERE dim2.eventTime IS NULL
    """)

    val query = transform.writeStream
      .format("memory")
      .queryName("result")
      .outputMode(OutputMode.Update())
      .start

    val result = spark.sql("SELECT * FROM result")

    addDim(ts(0), "A", 1)
    addDim(ts(0), "B", 2)
    query.processAllAvailable()

    addDim(ts(1), "B", 3)
    addFact(ts(1), "A")
    query.processAllAvailable()

    addDim(ts(2), "A", 4)
    addFact(ts(2), "B")
    query.processAllAvailable()

    addFact(ts(3), "A")
    query.processAllAvailable()

    addDim(ts(4), "A", 5)
    query.processAllAvailable()

    val expected = sc.parallelize(Seq(
      (ts(1), "A", Some(1)),
      (ts(2), "B", Some(3)),
      (ts(3), "A", Some(4))
    )).toDF("eventTime", "city", "population")

    assertDataFrameEquals(expected, result)
  }
}
