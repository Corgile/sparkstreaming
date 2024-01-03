package s.park;

import lombok.extern.slf4j.Slf4j;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import s.park.entity.AttackMessage;
import s.park.util.MySQLBatchWriter;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

@Slf4j
public class App2 {
  private static final Logger myErrorLogger = Logger.getLogger(App2.class);
  static MySQLBatchWriter sqlBatchWriter = new MySQLBatchWriter();

  public static void main(String[] args) throws StreamingQueryException, TimeoutException {

    SparkConf sparkConf = new SparkConf()
        .setAppName("FlowWarningCalculationJob")
        .set("spark.executor.heartbeatInterval", "18000ms")
        .set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .set("spark.sql.adaptive.enabled", "false")
        .set("failOnDataLoss", "false")
        .set("checkpointLocation", "hdfs://hadoop-master-146:8020/checkpoint")
        .setMaster("local")
        .setJars(new String[]{"hdfs://hadoop-master-146:8020/structured-streaming/jobs/latency-test.jar"});
//        .setMaster("yarn");

    SparkSession spark = SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate();

    // 订阅 Kafka 主题
    Dataset<Row> kafkaRawData = spark
        .readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers",
            "172.22.105.202:9092,172.22.105.203:9092," +
                "172.22.105.146:9092,172.22.105.147:9092," +
                "172.22.105.150:9092,172.22.105.38:9092," +
                "172.22.105.39:9092")
        .option("kafka.group.id", "spark_consumers_for_latency_test")
        .option("subscribe", "debug-warn")
        .option("failOnDataLoss", "false")
        .option("fetchOffset.numRetries", "3")
        .option("fetchOffset.retryIntervalMs", "1000")
        .load();


    StructType schema = Encoders.bean(AttackMessage.class).schema();
    Dataset<Row> mysqlDataFrame = kafkaRawData
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).as("data"))
        .select("data.*")
        .as(Encoders.bean(AttackMessage.class))
        .groupBy("srcIp", "dstIp", "attackType")
        .count()
        .toDF("srcIp", "dstIp", "attackType", "count");

    StreamingQuery writeToMySQL = mysqlDataFrame
        .writeStream()
        .foreach(sqlBatchWriter)
        .outputMode(OutputMode.Update())
        .start();

    /// key
    Dataset<Row> kafkaMessageKey = kafkaRawData
        .selectExpr("CAST(key AS STRING)");
    Dataset<Row> modifiedKey = kafkaMessageKey
        .withColumn("modifiedKey",
            concat(
                col("key"),
                lit(","),
                substring(col("key"), 42, 13),
                lit(","),
                lit(String.valueOf(System.currentTimeMillis()))
            ));

    String hdfsPath = "hdfs://hadoop-master-146:8020/user/wyyiot/timestamp.csv";
    StreamingQuery writeToHDFS = modifiedKey
        .select("modifiedKey")
        .writeStream()
        .outputMode(OutputMode.Append())
        .format("csv")
//        .format("console")
        .option("path", hdfsPath)
//        .foreachBatch(App2::call)
        .option("checkpointLocation", "hdfs://hadoop-master-146:8020/checkpoint")
        .start();

    log.warn("\033[34;1m ALL SET. \033[0m");
    writeToHDFS.awaitTermination();
    writeToMySQL.awaitTermination();
  }

  private static void call(Dataset<Row> batchDF, Long batchId) {
    batchDF.foreach((ForeachFunction<Row>) row -> myErrorLogger.error(row.getString(0)));
  }
}
