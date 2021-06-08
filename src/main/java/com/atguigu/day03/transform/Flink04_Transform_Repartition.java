package com.atguigu.day03.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_Transform_Repartition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(5);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).setParallelism(2);

        //KeyBy
        KeyedStream<String, String> keyedStream = map.keyBy(r -> r);
        //shuffle
        DataStream<String> shuffle = map.shuffle();
        //reblance
        DataStream<String> rebalance = map.rebalance();
        //rescale
        DataStream<String> rescale = map.rescale();

        map.print("原始数据").setParallelism(2);
        keyedStream.print("keyBy");
        shuffle.print("shuffle");
        rebalance.print("rebalance");
        rescale.print("rescale");

        env.execute();
    }
}
