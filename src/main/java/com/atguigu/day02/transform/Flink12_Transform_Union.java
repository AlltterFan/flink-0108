package com.atguigu.day02.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink12_Transform_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> elem1 = env.fromElements("a", "b", "c", "d", "e");
        DataStreamSource<String> elem2 = env.fromElements("1", "2", "3", "4", "5");

        //TODO Union
        DataStream<String> union = elem1.union(elem2);
        union.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value+"zzzzz";
            }
        }).print();
        env.execute();
    }
    /**
     *
     *
     * connect与 union 区别：
     * 1.	union之前两个流的类型必须是一样，connect可以不一样
     * 2.	connect只能操作两个流，union可以操作多个。
     */
}
