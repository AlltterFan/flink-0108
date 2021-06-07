package com.atguigu.day02.transform;

import com.atguigu.day02.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

public class Flink11_Transform_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> elem1 = env.fromElements("a", "b", "c", "d", "e");
        DataStreamSource<String> elem2 = env.fromElements("1", "2", "3", "4", "5");

        //TODO Connect

        ConnectedStreams<String, String> connect = elem1.connect(elem2);

        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<String, String, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value + "zzz";
            }

            @Override
            public String map2(String value) throws Exception {
                return value + "map2";
            }
        });


        map.print();
        env.execute();
    }
}
