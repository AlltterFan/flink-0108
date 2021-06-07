package com.atguigu.day02.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_Transform_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5);

//        SingleOutputStreamOperator<Integer> mapDStream = streamSource.map(new MyMap());
        //匿名实现类
        SingleOutputStreamOperator<Integer> mapDStream = streamSource.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * value;
            }
        });

        mapDStream.print();
        env.execute();
    }

    //map写法一：通过自定义类去实现MapFunction接口
    public static class MyMap implements MapFunction<Integer,Integer>{

        @Override
        public Integer map(Integer value) throws Exception {
            return value*value;
        }
    }
}
