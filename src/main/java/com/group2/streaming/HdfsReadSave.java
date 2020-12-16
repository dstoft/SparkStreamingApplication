package com.group2.streaming;

import com.group2.streaming.models.MappedTweet;
import com.group2.streaming.models.SentimentLists;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.json.JSONObject;
import scala.Tuple2;

import java.io.IOException;

public class HdfsReadSave {
    public static void main(String[] args) {
        //Create a DStream from this source - we listen for this repository
        String inputDirectory = "hdfs://10.123.252.244:9000/user/hadoop/stream-input/";

        //Create a local Streaming context with two working thread.
        //SparkConf sparkConf = new SparkConf().setAppName("HdfsStreamingReadSave");
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("HdfsStreamingReadSave");

        String lineSeparator = System.getProperty("line.separator");
        //Setup batch interval.
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(20000));
        ssc.sparkContext().hadoopConfiguration();

        JavaDStream<String> directoryStream = ssc.textFileStream(inputDirectory);

        Function<String, JSONObject> mapToJson = str -> {
            return new JSONObject(str);
        };


        Function<JSONObject, String> mapToMappedTweet = jsonObject -> {
            long id = jsonObject.getLong("id");
            boolean isRetweet = jsonObject.getBoolean("retweeted_status");
            boolean isTruncated = jsonObject.getBoolean("truncated");

            String extendedTweetRow = jsonObject.getString("extended_tweet");
            String user = jsonObject.getString("user");


            String text = jsonObject.getString("text");

            JSONObject entities;
            if (isTruncated) {
                entities = jsonObject.getJSONObject("extended_tweet").getJSONObject("entities");
                text = jsonObject.getJSONObject("extended_tweet").getString("full_text");
            } else {
                entities = jsonObject.getJSONObject("entities");
            }

            String timestampMs = jsonObject.getString("timestamp_ms");

            long friendsCount = jsonObject.getJSONObject("user").getLong("friends_count");
            boolean hasMentioned = entities.getBoolean("user_mentions");

            text = text.replace(lineSeparator, " ");

            SentimentLists wordsLists = SentimentService.MapWords(text);

            int sentimentCounter = wordsLists.positiveWords.size() - wordsLists.negativeWords.size();

            return String.valueOf(new MappedTweet(id, text, timestampMs, wordsLists.words, wordsLists.positiveWords, wordsLists.negativeWords, friendsCount, hasMentioned, sentimentCounter ));
        };

        Schema mappedTweetSchema = buildSchema();

        VoidFunction<JavaRDD<String>> saveRdd = rdd -> {
            // saveJavaRDD(rdd, mappedTweetSchema, "hdfs://10.123.252.244:9000/user/hadoop/stream-output/" + System.currentTimeMillis() + "/");
            rdd.saveAsTextFile("hdfs://10.123.252.244:9000/user/hadoop/stream-output/" + System.currentTimeMillis() + "/");
        };



        directoryStream.map(mapToJson).map(mapToMappedTweet)
                .foreachRDD(saveRdd);
        ssc.start();

        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Schema buildSchema() {
        Schema.Parser parser = new Schema.Parser();

        return parser.parse("{\n" +
                "\t\"namespace\":\"group2.avro\",\n" +
                "\t\"type\":\"record\",\n" +
                "\t\"name\":\"MappedTweet\",\n" +
                "\t\"fields\": [\n" +
                "\t\t{\"name\":\"id\",\"type\":\"long\"},\n" +
                "\t\t{\"name\":\"text\",\"type\":\"string\"},\n" +
                "\t\t{\"name\":\"timeInMs\",\"type\":\"long\"},\n" +
                "\t\t{\"name\":\"words\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"default\":[]},\n" +
                "\t\t{\"name\":\"positiveWords\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"default\":[]},\n" +
                "\t\t{\"name\":\"negativeWords\",\"type\":{\"type\":\"array\",\"items\":\"string\"},\"default\":[]},\n" +
                "\t\t{\"name\":\"friendsCount\",\"type\":\"long\"},\n" +
                "\t\t {\"name\":\"hasMentioned\",\"type\":\"boolean\"},\n" +
                "\t\t {\"name\":\"sentimentScore\",\"type\":\"int\"}\n" +
                "\t]\n" +
                "}");
    }

    private static Job getJob(Schema avroSchema) {

        Job job;

        try {
            job = Job.getInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        AvroJob.setOutputKeySchema(job, avroSchema);

        return job;
    }


    /**
     * Saves the given javaRDD as avro data with the given schema in a directory or file defined by path.
     */
    public static <T> void saveJavaRDD(JavaRDD<T> javaRDD, Schema avroSchema, String path) {
        JavaPairRDD<AvroKey<T>, NullWritable> javaPairRDD = javaRDD.mapToPair(r->new Tuple2<AvroKey<T>, NullWritable>(new AvroKey<T>(r), NullWritable.get()));
        saveJavaPairRDDKeys(javaPairRDD, avroSchema, path);

    }

    /**
     * Saves the keys from the given javaPairRDD as avro data with the given schema in a directory or file defined by path.
     */
    public static <K, V> void saveJavaPairRDDKeys(JavaPairRDD<K, V> javaPairRDD, Schema avroSchema, String path) {
        Job job = getJob(avroSchema);
        javaPairRDD.saveAsNewAPIHadoopFile(path, AvroKey.class, NullWritable.class, AvroKeyOutputFormat.class, job.getConfiguration());

    }
}
