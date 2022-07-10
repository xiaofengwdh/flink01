package org.wdh01.chapter05ForTest;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

public class SourceTest0614 {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取指定路径文件输出
/*        DataStreamSource<String> word = env.readTextFile("input/word.txt");
        word.print();*/
        //从sockert 读取数据实现 wordcount
       /* DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop103", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOne = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(new Tuple2<>(s1, 1));
                }
            }
        });
        wordOne.keyBy(data -> data.f0).sum(1).print();*/

        // env.fromElements(1,2,3,4,5).print();
        //env.fromCollection(new ArrayList{})
   /*     Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop103:9092");

        env.addSource(new FlinkKafkaConsumer<String>("lhc", new SimpleStringSchema(), properties)).print();
*/
        env.addSource(new MySource()).print();
        env.execute();
    }

    public static class MySource implements SourceFunction<Event> {
        private Boolean running = true;
        //随机范围
        String[] users = {"令狐冲", "依琳", "任盈盈", "莫大", "风清扬"};

        String[] urls = {"./home", "./cat", "./pay", "./info"};
        //随机生成数据
        Random random = new Random();

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            while (running) {
                String user = users[random.nextInt(users.length)];
                String url = urls[random.nextInt(urls.length)];
                long timeInMillis = Calendar.getInstance().getTimeInMillis();
                ctx.collect(new Event(user, url, timeInMillis));
                //慢一点：一分钟生成一次数据
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
