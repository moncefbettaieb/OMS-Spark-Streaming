package com.businessdecision.ocp;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.spark.api.java.JavaSparkContext;
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
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));

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
                try {
                    InputStream stream = new ByteArrayInputStream(
                            tuple2._2().getBytes()
                    );
                    BufferedInputStream in = new BufferedInputStream(stream);
                    XZCompressorInputStream xzIn = new XZCompressorInputStream(in);
                    final byte[] buffer = new byte[Integer.MAX_VALUE];
                    int n = 0;
                    String out = "";
                    while (-1 != (n = xzIn.read(buffer))) {
                        out = buffer.toString();
                        //out = out.write(buffer, 0, n);
                    }
                    JSONObject obj = new JSONObject(out);
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
                } catch (IOException e) {
                    e.printStackTrace();
                    return "";
                }
            }
        });
        lines.print();

        jssc.start();
        jssc.awaitTermination();
    }
}