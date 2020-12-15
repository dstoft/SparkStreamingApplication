package com.group2.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONObject;

public class HdfsReadSave {
    public static void main(String[] args) {
        String inputDirectory = "hdfs://10.123.252.244:9000/user/hadoop/stream-input/";

        SparkConf sparkConf = new SparkConf().setAppName("HdfsStreamingReadSave");

        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(20000));
        ssc.sparkContext().hadoopConfiguration();

        JavaDStream<String> directoryStream = ssc.textFileStream(inputDirectory);

        Function<String, JSONObject> mapToJson = str -> {
            return new JSONObject(str);
        };
        Function<JSONObject, String> mapCreatedAt = jsonObject -> {
            return jsonObject.getString("created_at");
        };

        directoryStream.map(mapToJson).map(mapCreatedAt)
                .dstream().saveAsTextFiles("hdfs://10.123.252.244:9000/user/hadoop/stream-output/" + System.currentTimeMillis() + "/", "Stream");
        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }
}
