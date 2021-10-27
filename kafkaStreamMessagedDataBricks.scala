import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, col, count, countDistinct, from_json, from_unixtime, get_json_object, split, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType, TimestampType}

object kafkaStreamMessagedDataBricks {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("kafkaStreamMessagedDataBricks").setMaster("local[2]")
    val spark = SparkSession.builder().config(conf = conf).getOrCreate()
    import spark.implicits._

    val streamingInputDF=spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "events")
      .option("startingOffsets","latest")
      .load()

    streamingInputDF.printSchema()

    val streaSelectDF=streamingInputDF.select(get_json_object(($"value").cast("string"),"$.uid").as("uid"),
      get_json_object(($"value").cast("string"),"$.ts").as("ts"),
      from_unixtime(get_json_object(($"value").cast("string"),"$.ts")).cast(TimestampType).as("eventdatetime"))

    //val aggDF=streaSelectDF.groupBy($"uid",window($"eventdatetime".as("timestamp"),"1 minute")).count()
    //val uniqustreaSelectDF=streaSelectDF.dropDuplicates()
    val JSONDF = streaSelectDF.withWatermark("eventdatetime", "1 minutes")
    val aggDF=JSONDF.groupBy(window($"eventdatetime".as("timestamp"),"1 minute")).agg(approx_count_distinct($"uid").as("uid_count"))

    aggDF.writeStream
      .trigger(Trigger.ProcessingTime("1 minute"))
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()
      .awaitTermination()

    spark.stop()
    spark.close()

  }


}
