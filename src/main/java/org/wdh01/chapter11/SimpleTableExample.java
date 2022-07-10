package org.wdh01.chapter11;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class SimpleTableExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //读取数据，提取水位线
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );
        //创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //将流转换table
        Table eventTable = tableEnv.fromDataStream(eventStream);
        //直接使用SQL
        Table resTable1 = tableEnv.sqlQuery("select user,url from " + eventTable);

        //通过表达式获取数据
        Table yilin = eventTable.select($("user"), $("url"))
                .where($("user").isEqual("依琳"));

        tableEnv.toDataStream(yilin).print("yilin   ");
        //将表转换回流
        tableEnv.toDataStream(resTable1).print();
        tableEnv.createTemporaryView("eventTable",eventTable);

        Table aggres = tableEnv.sqlQuery("select user ,count(url) as cnt from  eventTable group by user");

        tableEnv.toChangelogStream(aggres).print( "agg" );



        env.execute();
    }
}
