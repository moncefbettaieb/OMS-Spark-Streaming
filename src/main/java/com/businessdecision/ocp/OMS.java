package com.businessdecision.ocp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Pattern;

import kafka.producer.KeyedMessage;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.*;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.kafka.KafkaUtils;
import kafka.serializer.StringDecoder;

/**
 * Created by Moncef.Bettaieb on 19/04/2017.
 */
public final class OMS {
    private static final Pattern dot = Pattern.compile(",");
    private OMS() {
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: OMS Maintenance Spark Streaming <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }


        String brokers = args[0];
        String topics = args[1];

        // Create context with a 2 seconds batch interval
        SparkConf sparkConf = new SparkConf().setAppName("OMS Maintenance Spark Streaming");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);

        // Create direct kafka stream with brokers and topics
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );



        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> tuple2) {
                try {
                    JSONObject obj = new JSONObject(tuple2._2());
                    String status = obj.getString("status");
                    String date = obj.getJSONObject("end").getString("$date");
                    String gpk = "";
                    String rms = "";
                    JSONArray arr = obj.getJSONArray("globals");
                    gpk = arr.getString(0);
                    rms = arr.getString(1);
                    String pom = obj.getJSONObject("pom").getString("$oid");
                    String extTemp = obj.getJSONObject("values").getString("exttemp");
                    String taskId = obj.getJSONObject("taskid").getString("$oid");
                    String factor = obj.getString("factor");
                    String result = status+","+date+","+gpk+","+rms+","+pom+","+extTemp+","+taskId+","+factor;
                    Properties props = new Properties();
                    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.21.62.48:9092");
                    props.put(ProducerConfig.RETRIES_CONFIG, "3");
                    props.put(ProducerConfig.ACKS_CONFIG, "all");
                    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
                    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
                    props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
                    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                    KafkaProducer producer = new KafkaProducer<String,String>(props);
                    producer.send(new ProducerRecord<String, String>("events",
                            result));
                    return result;
                }
                catch (JSONException e){
                    return "" ;
                }
            }
        });

        lines.print();

        System.out.println(" line is  0------ 0"+ lines);



        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterable<String> call(String x) {
                return Lists.newArrayList(dot.split(x));
            }
        });
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
                new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(String s) {
                        return new Tuple2<String, Integer>(s, 1);
                    }
                }).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    public Integer call(Integer i1, Integer i2) {
                        return i1 + i2;
                    }
                });
        //wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }

}
