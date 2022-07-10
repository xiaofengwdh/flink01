package org.wdh01.chapter05;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wdh01.bean.Event;

public class TransfromRichFunctionTest {
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

        eventDStream.map(new MyMapRich()).setParallelism(2).print();
        env.execute();
    }

    public static class MyMapRich extends RichMapFunction<Event, Integer> {

        @Override
        public Integer map(Event value) throws Exception {
            return value.url.length();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open..." + getRuntimeContext().getIndexOfThisSubtask() + "   begin....");
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close..." + getRuntimeContext().getIndexOfThisSubtask() + "   end....");
        }
    }

}
