package com.businessdecision.ocp;

import java.sql.SQLException;
import java.util.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.json.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Time;


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

        final JavaSparkContext sc =
                new JavaSparkContext(new SparkConf().setAppName("OMS Maintenance Spark Streaming"));
        final SQLContext sqlContext = new SQLContext(sc);
        Class.forName("com.mysql.jdbc.Driver");

        final Map<String, String> options = new HashMap<String, String>();
        options.put("driver", DRIVER);
        options.put("url", URL + "?user=" + USERNAME + "&password=" + PASSWORD);
        options.put("dbtable", "Alert");

        final DataFrame df = sqlContext
                .read()
                .format("jdbc")
                .options(options)
                .load()
                .cache();
//                .persist(StorageLevel.MEMORY_AND_DISK_SER());

        // Looks the schema of this DataFrame.
        df.printSchema();
        df.show();
        //df.cache();

        final JavaPairRDD rdd2 = df.toJavaRDD().mapToPair(new PairFunction<Row, Integer, String>() {
            public Tuple2<Integer, String> call(Row row) throws Exception {
                return new Tuple2<Integer, String>(Integer.valueOf(row.getString(0)), row.getString(1));
            }
        });


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
                //df.show();
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

                    DataFrame df = sqlContext
                            .read()
                            .format("jdbc")
                            .options(options)
                            .load()
                            .cache();

//                    List<String> metrics = Arrays.asList(result.split(","));
                  //  JavaRDD<String> rdd1 = sc.parallelize(metrics);
                  //  JavaPairRDD<String,String> rdd0 = JavaPairRDD.fromJavaRDD(rdd1  );

                    JavaPairRDD<String, String> rdd2 = df.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
                        public Tuple2<String, String> call(Row row) throws Exception {
                            return new Tuple2<String, String>(row.getString(0), row.getString(1));
                        }
                    });

                    //sqlContext.sparkContext().parallelize(Seq(result)).toDF(result);

                    return result;
                }
                catch (JSONException e){
                    return "Json Exception : "+e ;
                }
            }
        });
        lines.print();

        //final JavaRDD<String> rdd2 = df.;

//        lines.foreachRDD((Function<JavaPairRDD<String, String>, Void>) rdd -> {
//            rdd.foreachPartition(tuple2Iterator -> {
//                // get message
//                Tuple2<String, String> item = tuple2Iterator.next();
//
//                // lookup
//                String sqlQuery = "SELECT something FROM somewhere";
//                Seq<String> resultSequence = hiveContext.runSqlHive(sqlQuery);
//                List<String> result = scala.collection.JavaConversions.seqAsJavaList(resultSequence);
//
//            });
//            return null;
//        });
//
//        lines.foreach(new Function<JavaPairRDD<String, String>, Void>() {
//
//            public Void call(JavaPairRDD<String, String> stringStringJavaPairRDD) throws Exception {
//                return null;
//            }
//
//        });


        //rdd =>rdd.join(geoData))
        //lines.tran

        lines.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
            public Void call(JavaRDD<String> rdd, Time time)
                    throws SQLException {
                JavaPairRDD<String, Integer> rddpair1 = rdd.mapToPair(new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(final String readName) {
                        return new Tuple2<String, Integer>(readName, Integer.valueOf(1));
                    }
                });
                JavaPairRDD<String, Integer> rddpair2 = rdd2.mapToPair(new PairFunction<String, String, Integer>() {
                    public Tuple2<String, Integer> call(final String readName) {
                        return new Tuple2<String, Integer>(readName, Integer.valueOf(1));
                    }
                });

                JavaPairRDD rr = rddpair1.join(rddpair2);
                rr.foreach(new VoidFunction<Tuple2<String, String>>() {
                    public void call(Tuple2<String, String> t) throws Exception {
                        System.out.println(t._1() + " " + t._2());
                    }
                });
                return null;
            }
        });
//
//        lines.foreach(new Function<JavaPairRDD<String, String>, Void>() {
//            public Void call(JavaPairRDD<String, String> rdd) {
//                JavaPairRDD<String, Tuple2> joinRdd = rdd.join(rdd2);
//                return (null);
//            }
//        });

//        JavaPairRDD<String, String> firstRDD = rdd1.mapToPair(new PairFunction<String, String, String>() {
//            public Tuple2<String, String> call(String s) {
//                String[] customerSplit = s.split(",");
//                return new Tuple2<String, String>(customerSplit[0], customerSplit[1]);
//            }
//        });
        //JavaPairRDD<String,String> firstRDD = sc.parallelizePairs(List<"test",result>);
//        JavaPairRDD<String,String> secondRDD = rdd2.mapToPair(new PairFunction<String, String, String>() {
//            public Tuple2<String, String> call(String s) {
//                String[] customerSplit = s.split(",");
//                return new Tuple2<String, String>(customerSplit[0], customerSplit[1]);
//            }
//        });

//        JavaPairRDD<String, Tuple2<String, String>> joinsOutput = firstRDD.join(secondRDD);

//        JavaDStream<String> test = lines.reduceByWindow(new Function2<String, String, String>() {
//            public String call(String s, String s2) throws Exception {
//                return null;
//            }
//        },Durations.seconds(5),Durations.seconds(5));
//
//
//        JavaPairDStream<String, String> windowedStream1 = null;
//
//        JavaPairDStream<String, Tuple2<String, String>> joinedStream = windowedStream1.join(windowedStream1);

        //JavaRDD mysql = new JdbcRDD<String>();

        jssc.start();
        jssc.awaitTermination();
    }

}