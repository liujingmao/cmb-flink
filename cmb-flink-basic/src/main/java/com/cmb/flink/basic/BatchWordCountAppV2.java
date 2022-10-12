package com.cmb.flink.basic;

import com.cmb.flink.basic.function.CMBFlatMapFunction;
import com.cmb.flink.basic.function.CMBMap;
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
        dataSource.flatMap(new CMBFlatMapFunction()).map(new CMBMap())
                .groupBy(0)
                .sum(1)
              .print();

    }
}
