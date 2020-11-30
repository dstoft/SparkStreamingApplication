package com.group2.readhdfs;

import org.apache.avro.generic.GenericRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import parquet.hadoop.ParquetInputFormat;
import scala.Tuple2;

import java.util.Arrays;

public class HdfsReadSave {
    public static void main(String[] args) {
//        SparkConf sparkConf = new SparkConf().setAppName("HdfsStreamingReadSave");
//
//        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(2000));
//        ssc.sparkContext().hadoopConfiguration().set("parquet.read.support.class", "parquet.avro.AvroReadSupport");

//        ssc.fileStream<Void, GenericRecord, ParquetInputFormat<GenericRecord>>()
//        ssc.fileStream("", Void, GenericRecord, ParquetInputFormat<GenericRecord>)
//        ssc.fileStream("", String.class, GenericRecord.class, ParquetInputFormat.class)
//        ssc.fileStream<String, GenericRecord, ParquetInputFormat>

//        JavaPairInputDStream<Void, GenericRecord> lines = ssc.fileStream("hdfs://10.123.252.244:9000/user/hadoop/twitter-files-streaming/*.parquet", void.class, GenericRecord.class, ParquetInputFormat.class);
//        JavaDStream<String> words = lines.flatMap((line) ->
//            Arrays.asList(line._2.get(2).toString().split(" ")).iterator());
//        words.dstream().saveAsTextFiles("hdfs://10.123.252.244:9000/user/hadoop/twitter-files-stream-output/", "words.txt");
//        JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));
//        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);
//        wordCounts.print();
//        wordCounts.dstream().saveAsTextFiles("hdfs://10.123.252.244:9000/user/hadoop/twitter-files-stream-output/", "wordcounts");



        SparkSession spark = SparkSession.builder().appName("HdfsReadSave").getOrCreate();
        Dataset<Row> dataSchema = spark.read().parquet("hdfs://10.123.252.244:9000/user/hadoop/twitter-files-streaming/*.parquet");
        Dataset<Row> streamingDataFrame = spark.readStream().schema(dataSchema.schema()).option("maxFilesPerTrigger", "1").option("header", "true").parquet("hdfs://10.123.252.244:9000/user/hadoop/twitter-files-streaming/*.parquet");
//
        streamingDataFrame.select("text").write().text("hdfs://10.123.252.244:9000/user/hadoop/twitter-files-stream-output/texts");


    }
}
