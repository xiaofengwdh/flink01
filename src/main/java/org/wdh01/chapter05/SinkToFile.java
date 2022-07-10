package org.wdh01.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.wdh01.bean.Event;

import java.util.concurrent.TimeUnit;

public class SinkToFile {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从元素中读取数据
        DataStreamSource<Event> eventDStream = env.fromElements(
                new Event("令狐冲", "/home", 1000L),
                new Event("依琳", "/cat", 9000L),
                new Event("任盈盈", "/pay", 8000L),
                new Event("依琳", "/info?id=2", 8000L),
                new Event("任盈盈", "/home", 8000L),
                new Event("依琳", "/error", 100000L)
        );


        StreamingFileSink build = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                new SimpleStringEncoder("UTF-8")) //指定字符编码
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024) //滚动新文件大小限制：1G开启一个文件
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))//滚动新文件时间限制：多久开启一个文件
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))//5分钟不写入数据直接保存
                                .build()).build();


        eventDStream.map(date->date.toString()).addSink(build);

        env.execute();

    }
}
