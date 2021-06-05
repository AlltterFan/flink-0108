package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //1.创建环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读取数据
        DataSource<String> dataSource = env.readTextFile("input/word.txt");

        //3.先将一行数据按照空格切分，切成一个一个单词，并且把这些单词组成Tuple元组（word,1）
        //flatMap
//        MyFlatMapFun myFlatMapFun = new MyFlatMapFun();
        FlatMapOperator<String, Tuple2<String, Long>> flatMap = dataSource.flatMap((FlatMapFunction<String, Tuple2<String, Long>>) (value, out) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.LONG));

        //4.将相同Key的数据聚和到一块
        UnsortedGrouping<Tuple2<String, Long>> groupBy = flatMap.groupBy(0);

        //5.累加
        AggregateOperator<Tuple2<String, Long>> sum = groupBy.sum(1);

        //6.打印
        sum.print();
    }

/*    public static class MyFlatMapFun implements FlatMapFunction<String, Tuple2<String,Long>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
            //将读过来的一行一行数据按空格切分，切成一个一个单词
            String[] words = value.split(" ");
            for (String word : words) {
//                Tuple2.of(word, 1L);
                out.collect( Tuple2.of(word, 1L));
            }

        }
    }*/
}
