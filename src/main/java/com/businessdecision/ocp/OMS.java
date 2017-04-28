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
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.google.common.collect.Lists;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import scala.Tuple2;

public final class OMS {
    private static final Pattern dot = Pattern.compile(" ");

    public static void main(String[] args) throws ClassNotFoundException {
        if (args.length < 2) {
            System.err.println("Usage: OMS Maintenance Spark Streaming <brokers> <topics>\n" +
                    "  <brokers> is a list of one or more Kafka brokers\n" +
                    "  <topics> is a list of one or more kafka topics to consume from\n\n");
            System.exit(1);
        }
        Class.forName("com.mysql.jdbc.Driver");
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName(
                "OMS Maintenance Spark Streaming");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
                Durations.seconds(1));

        String brokers = args[0];
        String topics = args[1];
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.21.62.48:9092");
        props.put(ProducerConfig.RETRIES_CONFIG, "3");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "none");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);
        props.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, true);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

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

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            public String call(Tuple2<String, String> tuple2) {
                try {
// TODO add decompression
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
                    String result = status + "," + date + "," + gpk + "," + rms + "," + pom + "," + extTemp + "," + taskId + "," + factor;

                    KafkaProducer producer = new KafkaProducer<String, String>(props);
                    producer.send(new ProducerRecord<String, String>("events",
                            result));
                    return result;
                } catch (JSONException e) {
                    return "Json Exception : " + e;
                }
            }
        });

        JavaDStream<String> words = lines
                .flatMap(new FlatMapFunction<String, String>() {
                    public Iterable<String> call(String x) {
                        return Lists.newArrayList(dot.split(x));
                    }
                });

        words.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
            public Void call(JavaRDD<String> rdd, Time time)
                    throws SQLException {
                Connection mcConnect = null;
                PreparedStatement st = null;
                try {
                    mcConnect = DriverManager.getConnection(
                            "jdbc:mysql://10.21.62.49/ocp_maint", "root", "SPLXP026");
                    String query = "SELECT * FROM Alert WHERE idPom = ?";
                    List<String> list = rdd.collect();
                    if (list.size() > 0) {
                        for (String value : list) {
                            System.out.format("%s\n", value);
                            st = mcConnect.prepareStatement(query);
                            String[] values = value.split(",");
                            if (values.length > 4) {
                                String Alert = "";
                                String pom = values[4];
                                Float gpk = Float.valueOf(values[2]);
                                Float rms = Float.valueOf(values[3]);
                                Float temperature = Float.valueOf(values[5]);
                                st.setString(1, pom);
                                ResultSet rs = st.executeQuery();
                                while (rs.next()) {
                                    int idArlert = rs.getInt("idAlert");
                                    String idPom = rs.getString("idPom");
                                    Date dateAlert = rs.getDate("dateAlert");
                                    Float gpkAlertMax = rs.getFloat("GpkAlertMax");
                                    Float gpkAlertMin = rs.getFloat("GpkAlertMin");
                                    Float gpkEmergMax = rs.getFloat("GpkEmergMax");
                                    Float gpkEmergMin = rs.getFloat("GpkEmergMin");
                                    Float tempAlertMax = rs.getFloat("TempAlertMax");
                                    Float tempAlertMin = rs.getFloat("TempAlertMin");
                                    Float tempEmergMax = rs.getFloat("TempEmergMax");
                                    Float tempEmergMin = rs.getFloat("TempEmergMin");
                                    Float rmsAlertMax = rs.getFloat("RmsAlertMax");
                                    Float rmsAlertMin = rs.getFloat("RmsAlertMin");
                                    Float rmsEmergMax = rs.getFloat("RmsEmergMax");
                                    Float rmsEmergMin = rs.getFloat("RmsEmergMin");
                                    System.out.format("%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s, %s, %s, %s\n", idArlert, idPom, dateAlert,
                                            gpkAlertMax, gpkAlertMin, gpkEmergMax, gpkEmergMin,
                                            tempAlertMax, tempAlertMin, tempEmergMax, tempEmergMin,
                                            rmsAlertMax, rmsAlertMin, rmsEmergMax, rmsEmergMin);

                                    Alert += idPom + "," + String.valueOf(dateAlert);
                                    if (temperature >= tempAlertMax) Alert += "," + String.valueOf("1");
                                    else Alert += "," + String.valueOf("0");
                                    if (temperature <= tempAlertMin) Alert += "," + String.valueOf("1");
                                    else Alert += "," + String.valueOf("0");
                                    if (temperature >= tempEmergMax) Alert += "," + String.valueOf("1");
                                    else Alert += "," + String.valueOf("0");
                                    if (temperature <= tempEmergMin) Alert += "," + String.valueOf("1");
                                    else Alert += "," + String.valueOf("0");
                                    if (gpk >= gpkAlertMax) Alert += "," + String.valueOf("1");
                                    else Alert += "," + String.valueOf("0");
                                    if (gpk <= gpkAlertMin) Alert += "," + String.valueOf("1");
                                    else Alert += "," + String.valueOf("0");
                                    if (gpk >= gpkEmergMax) Alert += "," + String.valueOf("1");
                                    else Alert += "," + String.valueOf("0");
                                    if (gpk <= gpkEmergMin) Alert += "," + String.valueOf("1");
                                    else Alert += "," + String.valueOf("0");
                                    if (rms >= rmsAlertMax) Alert += "," + String.valueOf("1");
                                    else Alert += "," + String.valueOf("0");
                                    if (rms <= rmsAlertMin) Alert += "," + String.valueOf("1");
                                    else Alert += "," + String.valueOf("0");
                                    if (rms >= rmsEmergMax) Alert += "," + String.valueOf("1");
                                    else Alert += "," + String.valueOf("0");
                                    if (rms <= rmsEmergMin) Alert += "," + String.valueOf("1");
                                    else Alert += "," + String.valueOf("0");
                                    KafkaProducer producer = new KafkaProducer<String, String>(props);
                                    producer.send(new ProducerRecord<String, String>("alerts",
                                            Alert));
                                }
                            }
                        }
                    }
                } finally {
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
