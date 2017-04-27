package com.businessdecision.ocp;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

/**
 * Created by Moncef.Bettaieb on 27/04/2017.
 */


public class OMSXZ {

    public static void main(String[] args) throws Exception{

        SparkConf conf = new SparkConf().setAppName("Example");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        Configuration configuration = new Configuration();
        configuration.set("io.compression.codecs","io.sensesecure.hadoop.xz.XZCodec");

        JavaPairRDD<Text, Text> inputRDD = jsc.newAPIHadoopFile("/user/moncef.bettaeib/stream/2017.04.27.17.11.14/part-00000",TextInputFormat.class,Text.class,Text.class, configuration);

        System.out.println(inputRDD.count());

//        rdd.foreach(new VoidFunction<String>(){
//            public void call(String record) throws Exception {
//                System.out.println("Record==>"+record);
//
//            }});
    }

}
