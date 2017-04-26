package com.businessdecision.ocp;

import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.regex.Pattern;

import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Lists;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import scala.Tuple2;

public final class OMSTest {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws ClassNotFoundException {
        if (args.length < 2) {
            System.err.println("Usage: OMS Maintenance Spark Streaming <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName(
                "OMS Maintenance Spark Streaming");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
                Durations.seconds(1));

        Class.forName("com.mysql.jdbc.Driver");

        String brokers = args[0];
        String topics = args[1];

        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                ssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );


//        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(args[0],
//                Integer.parseInt(args[1]));

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> tuple2) {
                //df.show();



                try {

//                    InputStream stream = new ByteArrayInputStream(
//                            tuple2._2().getBytes("UTF-8")
//                    );
//                    XZInputStream inxz = new XZInputStream(stream);
//                    //outString = inxz.toString();
//
//                    byte firstByte = (byte) inxz.read();
//                    byte[] buffer = new byte[inxz.available()];
//                    buffer[0] = firstByte;
//                    inxz.read(buffer, 1, buffer.length - 2);
//                    inxz.close();
//                    return new String(buffer);

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

//                    DataFrame df = sqlContext
//                            .read()
//                            .format("jdbc")
//                            .options(options)
//                            .load()
//                            .cache();
//
////                    List<String> metrics = Arrays.asList(result.split(","));
//                  //  JavaRDD<String> rdd1 = sc.parallelize(metrics);
//                  //  JavaPairRDD<String,String> rdd0 = JavaPairRDD.fromJavaRDD(rdd1  );
//
//                    JavaPairRDD<String, String> rdd2 = df.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
//                        public Tuple2<String, String> call(Row row) throws Exception {
//                            return new Tuple2<String, String>(row.getString(0), row.getString(1));
//                        }
//                    });

                    //sqlContext.sparkContext().parallelize(Seq(result)).toDF(result);

                    return result;
                }
                catch (JSONException e) {
                    return "Json Exception : " + e;
                }
//                } catch (UnsupportedEncodingException e) {
//                    e.printStackTrace();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
            }
        });

        JavaDStream<String> words = lines
                .flatMap(new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String x) {
                        return Lists.newArrayList(SPACE.split(x));
                    }
                });
        // Convert RDDs of the words DStream to DataFrame and run SQL query
        words.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
            public Void call(JavaRDD<String> rdd, Time time)
                    throws SQLException {

                Connection mcConnect = null;
                Statement st = null;
                //PreparedStatement mStatement = null;
                try {
                    mcConnect = DriverManager.getConnection(
                            "jdbc:mysql://10.21.62.49/ocp_maint", "root", "SPLXP026");
                    String query = "select * from Alert";
                    st = mcConnect.createStatement();
                    //mStatement = st.prepareStatement("select * from Alert");


                    List<String> list =rdd.collect();


                    if (list.size()>0) {

                        ResultSet rs = st.executeQuery(query);
                        while (rs.next())
                        {
                            int idArlert = rs.getInt("idAlert");
                            String idPom = rs.getString("idPom");
                            Date dateAlert = rs.getDate("dateAlert");
                            Float GpkAlertMax = rs.getFloat("GpkAlertMax");
                            Float GpkAlertMin = rs.getFloat("GpkAlertMin");
                            Float GpkEmergMax = rs.getFloat("GpkEmergMax");
                            Float GpkEmergMin = rs.getFloat("GpkEmergMin");
                            Float TempAlertMax = rs.getFloat("TempAlertMax");
                            Float TempAlertMin = rs.getFloat("TempAlertMin");
                            Float TempEmergMax = rs.getFloat("TempEmergMax");
                            Float TempEmergMin = rs.getFloat("TempEmergMin");
                            Float RmsAlertMax = rs.getFloat("RmsAlertMax");
                            Float RmsAlertMin = rs.getFloat("RmsAlertMin");
                            Float RmsEmergMax = rs.getFloat("RmsEmergMax");
                            Float RmsEmergMin = rs.getFloat("RmsEmergMin");



                            // print the results
                            System.out.format("%s, %s, %s, %s, %s, %s\n", idArlert, idPom, dateAlert, GpkAlertMax, GpkAlertMin, GpkEmergMax);
                        }
                        for(String value : list){
                            System.out.format("%s\n", value);
                            //mStatement.setString(1, value);
                            //mStatement.setString(2, "1");
                            //mStatement.executeUpdate();
                        }
                    }
                }
                finally {
                    if (mcConnect != null) {
                        mcConnect.close();
                    }
                    if (st != null) {
                        st.close();
                    }
                }

                return null;
            }
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
