package com.businessdecision.ocp;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;


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

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });
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
        wordCounts.print();

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

}
