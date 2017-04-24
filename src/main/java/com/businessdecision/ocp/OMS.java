package com.businessdecision.ocp;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.json.*;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
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

        SparkConf sparkConf = new SparkConf().setAppName("OMS Maintenance Spark Streaming");
        JavaSparkContext conf = new JavaSparkContext(sparkConf);
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);

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
                String outString = "";
                try {
                    InputStream stream = new ByteArrayInputStream(
                            tuple2._2().getBytes()
                    );

                    BufferedInputStream in = new BufferedInputStream(stream);
                    XZCompressorInputStream xzIn = new XZCompressorInputStream(in);
                    ByteArrayOutputStream  out = new ByteArrayOutputStream();
                    final byte[] buffer = new byte[Integer.MAX_VALUE];
                    int n = 0;

                    while (-1 != (n = xzIn.read(buffer))) {
                        //out += buffer.toString();
                        //buffer.toString();
                        out.write(buffer, 0, n);
                        //outString = buffer.();

                    }
                    String aString = new String(out.toByteArray(),"UTF-8");
                    return aString;
                } catch (IOException e) {
                    System.out.println(e);
                    e.printStackTrace();
                    return e.getMessage();
                }
            }
        });
        lines.print();

//        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
//             public Iterable<String> call(String x) {
//                                return Lists.newArrayList(dot.split(x));
//                            }
//         });
//                JavaPairDStream<String, Integer> wordCounts = words.mapToPair(
//                                new PairFunction<String, String, Integer>() {
//                     public Tuple2<String, Integer> call(String s) {
//                                                return new Tuple2<String, Integer>(s, 1);
//                                            }
//                 }).reduceByKey(
//                                new Function2<Integer, Integer, Integer>() {
//                     public Integer call(Integer i1, Integer i2) {
//                                               return i1 + i2;
//                                            }
//                 });
//                wordCounts.print();

        jssc.start();
        jssc.awaitTermination();
    }
}