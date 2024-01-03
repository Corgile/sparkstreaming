package s.park;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
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

import java.io.Serializable;
import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;

@Slf4j
public class App implements Serializable {

  static MySQLBatchWriter sqlBatchWriter = new MySQLBatchWriter();

  public static void main(String[] args) throws TimeoutException, StreamingQueryException {
    log.warn("\033[31;1m starting spark calculation job...\033[0m");
    SparkConf sparkConf = new SparkConf()
        .setAppName("FlowWarningCalculationJob")
        .set("spark.executor.heartbeatInterval", "18000ms")
        .set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .set("spark.sql.adaptive.enabled", "false")
        .set("checkpointLocation", "/structured-streaming/checkpoint")
        .setJars(new String[]{
            "hdfs://hadoop-master-146:8020/structured-streaming/jobs/warn-statistics.jar"
        })
        .setMaster("local");
//    .setMaster("yarn");

    SparkSession sparkSession = SparkSession
        .builder()
        .config(sparkConf)
        .getOrCreate();

    Dataset<Row> kafkaResource = sparkSession.readStream()
        .format("kafka")
        .option("kafka.bootstrap.servers",
                "172.22.105.202:9092,172.22.105.203:9092," +
                "172.22.105.146:9092,172.22.105.147:9092," +
                "172.22.105.150:9092,172.22.105.38:9092," +
                "172.22.105.39:9092")
        .option("kafka.group.id", "spark_consumers")
        .option("subscribe", "flow-warning-msg")
        .option("fetchOffset.numRetries", "3")
        .option("fetchOffset.retryIntervalMs", "1000")
        .load();

    log.warn("\033[32;1m Config JSON resolving ...\033[0m");
    StructType schema = Encoders.bean(AttackMessage.class).schema();
    Dataset<AttackMessage> parsedData = kafkaResource
        .selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).as("data"))
        .select("data.*")
        .as(Encoders.bean(AttackMessage.class));

    log.warn("\033[33;1m Config grouping columns ...\033[0m");
    Dataset<Row> dataFrame = parsedData
        .groupBy("srcIp", "dstIp", "attackType")
        .count()
        .toDF("srcIp", "dstIp", "attackType", "count");

    log.warn("\033[34;1m Config MySQL writer ...\033[0m");
    StreamingQuery started = dataFrame
        .writeStream()
        .foreach(sqlBatchWriter)
        .outputMode(OutputMode.Update())
        .start();

    log.warn("\033[33;1m Job started, awating for termination... \033[0m");
    started.awaitTermination();
  }
}
