package com.xuehui.tutorial;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;


//import com.tantan.entity.summary.Summary;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by chenxuehui on 2020/7/22
 */
public class WindowFunctionTests {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        List<String> list = new ArrayList<>();
        int N = 10;
        int M = 4;
        for (int j = 0; j < M; j++) {
            for (int i = 0; i < N; i++) {
                list.add(j + "_" + i);
            }
        }
        DataStream<String> source = env
//                .setParallelism(3)
                .fromCollection(list);

        KeyedStream<Tuple2<String, String>, Tuple> keyedStream =
                source.map(x -> Tuple2.of(x.split("_")[1], x))
                        .returns(Types.TUPLE(Types.STRING, Types.STRING))
//                        .setParallelism(3)
                        .keyBy(0);

        keyedStream
                //.window(TumblingProcessingTimeWindows.of(Time.milliseconds(1)))
                .countWindow(4)

//                .aggregate(new AggregateFunction<Tuple2<String, String>, String, String>() {
//                    @Override
//                    public String createAccumulator() {
//                        return "_";
//                    }
//
//                    @Override
//                    public String add(Tuple2<String, String> value, String accumulator) {
//                        //return value.f0 + accumulator;
//                        return "[" + value.f0 + "," + accumulator + "]";
//                    }
//
//                    @Override
//                    public String merge(String a, String b) {
//                        //return a + b;
//                        return "{" + a + "," + b + "}";
//                    }
//
//                    @Override
//                    public String getResult(String accumulator) {
//                        return accumulator;
//                    }
//                })

                .reduce(new ReduceFunction<Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> reduce(Tuple2<String, String> value1, Tuple2<String, String> value2) throws Exception {
                        return new Tuple2<>("[" + value1.f0 + "," + value2.f0 + "]", "[" + value1.f1 + "," + value2.f1 + "]");
                    }
                })
//                .setParallelism(3)
                .print();
        env.execute();

    }

//    public interface HelloInterface {
//        void sayHello();
//    }
//
//    public static class Hello implements HelloInterface{
//        @Override
//        public void sayHello() {
//            System.out.println("Hello !");
//        }
//    }
//
//    public static class InvokeHandler implements InvocationHandler{
//        private Object object;
//        public InvokeHandler(Object object){
//            this.object = object;
//        }
//        @Override
//        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
//            System.out.println("Before invoke "  + method.getName());
//            method.invoke(object, args);
//            System.out.println("After invoke " + method.getName());
//            return null;
//        }
//    }
//
//    public static void main(String[] args) {
//        System.getProperties().setProperty("sun.misc.ProxyGenerator.saveGeneratedFiles", "true");
//
//        HelloInterface hello = new Hello();
//
//        InvocationHandler handler = new InvokeHandler(hello);
//
//        HelloInterface proxyHello = (HelloInterface) Proxy.newProxyInstance(hello.getClass().getClassLoader(), hello.getClass().getInterfaces(), handler);
//
//        proxyHello.sayHello();
//    }
}

