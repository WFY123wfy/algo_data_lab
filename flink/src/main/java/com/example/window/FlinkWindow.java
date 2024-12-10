package com.example.window;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * flink窗口测试
 */
@Slf4j
public class FlinkWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        source.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<String>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                return Integer.parseInt(element.split("\\|")[2]);
                            }
                        })
                ).map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String value) throws Exception {
                        String[] split = value.split("\\|");
                        return new Event(split[0], Integer.parseInt(split[1]), Long.parseLong(split[2]));
                    }
                }).keyBy(Event::getName)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new MyAggregateFunction(), new ProcessWindowFunction<String, Object, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<String, Object, String, TimeWindow>.Context context, Iterable<String> elements, Collector<Object> out) throws Exception {
                        System.out.println("elements: " + elements);
                        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                        out.collect("sbutask: " + indexOfThisSubtask + ", value: " + elements);
                    }
                })
                .print();

        env.execute();
    }
}

@Data
class Event{
    private String name;
    private int age;
    long timestamp;

    public Event() {
    }

    public Event(String name, int age, long timestamp) {
        this.name = name;
        this.age = age;
        this.timestamp = timestamp;
    }
}

class MyAggregateFunction implements AggregateFunction<Event, String, String> {

    @Override
    public String createAccumulator() {
        return "";
    }

    @Override
    public String add(Event value, String accumulator) {
        if (accumulator.isEmpty()) {
            return String.format("%s|%s", value.getName(), value.getAge());
        }

        String[] split = accumulator.split("\\|");
        String name = split[0];
        String age = split[1];
        String ret = String.format("%s|%s", name, Integer.parseInt(age) + value.getAge());
        System.out.println("add: " + ret);
        return ret;
    }

    @Override
    public String getResult(String accumulator) {
        System.out.println("accumulator: " + accumulator);
        return accumulator;
    }

    @Override
    public String merge(String a, String b) {
        return null;
    }
}