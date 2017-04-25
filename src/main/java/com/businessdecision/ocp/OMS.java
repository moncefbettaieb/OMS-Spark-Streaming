package com.businessdecision.ocp;

import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.JdbcRDD;
import org.apache.spark.sql.DataFrame;
import org.json.*;
import org.apache.spark.sql.SQLContext;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.kafka.KafkaUtils;
import kafka.serializer.StringDecoder;
import com.mysql.jdbc.*;

/**
 * Created by Moncef.Bettaieb on 19/04/2017.
 */
public final class OMS {

    public static final String DRIVER = "com.mysql.jdbc.Driver";
    public static final String URL = "jdbc:mysql://10.21.62.49:3306/ocp_maint";
    public static final String USERNAME = "root";
    public static final String PASSWORD = "SPLXP026";

    private OMS() {
    }

    public static void main(String[] args) throws ClassNotFoundException {
        if (args.length < 2) {
            System.err.println("Usage: OMS Maintenance Spark Streaming <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }


        JavaSparkContext sc =
                new JavaSparkContext(new SparkConf().setAppName("Spark Example"));
        SQLContext sqlContext = new SQLContext(sc);
        Class.forName("com.mysql.jdbc.Driver");

        Map<String, String> options = new HashMap<String, String>();
        options.put("driver", DRIVER);
        options.put("url", URL + "?user=" + USERNAME + "&password=" + PASSWORD);
        options.put("dbtable", "Alert");

        final DataFrame df = sqlContext
                .read()
                .format("jdbc")
                .options(options)
                .load();

// Looks the schema of this DataFrame.
        df.printSchema();
        df.show();

        String brokers = args[0];
        String topics = args[1];

        //SparkConf sparkConf = new SparkConf().setAppName("OMS Maintenance Spark Streaming");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));

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
                df.show();
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
                    return "Json Exception : "+e ;
                }
            }
        });
        lines.print();

        //JavaRDD mysql = new JdbcRDD<String>();

        jssc.start();
        jssc.awaitTermination();
    }

}