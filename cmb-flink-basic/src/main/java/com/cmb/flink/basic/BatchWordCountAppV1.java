package com.cmb.flink.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Java 版本Flink WordCount
 */

public class BatchWordCountAppV1 {

    public static void main(String[] args) throws Exception {
        //1. Get env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2. 从本地文件读取文件数据
        DataSource<String> dataSource = env.readTextFile("/Users/liujingmao/IdeaProjects/cmb-flink/data/data.txt");
        //3. 将一行行数据flat
        dataSource.flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        //value是输入，out是输出
                        String[] splits = value.split(",");
                        for (String split : splits) {
                            out.collect(split.toLowerCase().trim());
                        }
                    }
                }).map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        //value是输入，Tuple2是输出
                        return Tuple2.of(value, 1);
                    }
                }).groupBy(0)
                .sum(1)
              .print();

    }
}
