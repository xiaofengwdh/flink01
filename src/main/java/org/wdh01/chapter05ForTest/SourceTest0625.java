package org.wdh01.chapter05ForTest;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.wdh01.bean.Event;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;
import java.util.Random;

/**
 * 源算子复习
 */
public class SourceTest0625 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
/*        //方式1、读取文件
        DataStreamSource<String> fileStreamSource = env.readTextFile("input/clicks.txt");
        fileStreamSource.print("  fileStreamSource    ");
        //方式2、读取集合数据
        ArrayList<Event> event = new ArrayList<>();
        event.add(new Event("令狐冲", "/info", 10000L));
        event.add(new Event("依琳", "/cat", 20000L));

        DataStreamSource<Event> collectionStream = env.fromCollection(event);
        collectionStream.print("  collectionStream   ");
        //方式3 从元素中读取数据
        DataStreamSource<Event> elemantStream =
                env.fromElements(new Event("令狐冲", "/info", 10000L),
                        new Event("依琳", "/cat", 20000L));
        elemantStream.print("   elemantStream   ");

        //方式4 读取socket
        DataStreamSource<String> sockettream = env.socketTextStream("hadoop103", 9999);
        sockettream.print("   sockettream   ");*/

        //方式5 读取kafka
  /*      Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop103:9092");
        DataStreamSource<String> kafkasStream = env.addSource(new FlinkKafkaConsumer<String>("lhc", new SimpleStringSchema(), properties));
        kafkasStream.print(" kafkasStream ");
        */

        //方式6 自定义 source
        // env.addSource(new MySource0625()).setParallelism(2).print();
         env.addSource(new MyParaiSource()).setParallelism(2).print();


        env.execute();

    }

    //自定义source;注意 SourceFunction 并行度1，只能是1
    public static class MySource0625 implements SourceFunction<Event> {
        //是否执行
        private boolean ifRun = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            Random random = new Random();
            String[] users = {"令狐冲", "依琳", "莫大", "风清扬", "任盈盈", "林远图", "定仪"};
            String[] urls = {"/home", "/cart", "/pay", "/info?id?"};
            while (ifRun) {
                //直接返回一个对象
                ctx.collect(new Event(users[random.nextInt(urls.length)],
                        urls[random.nextInt(urls.length)],
                        Calendar.getInstance().getTimeInMillis()));
                //1s  生成一条
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            //取消
            ifRun = false;
        }
    }

    //可并行source
    public static class MyParaiSource implements ParallelSourceFunction<Event> {

        private boolean ifRun = true;

        @Override
        public void run(SourceContext<Event> ctx) throws Exception {
            String[] users = {"令狐冲", "依琳", "莫大", "风清扬", "任盈盈", "林远图", "定仪"};
            String[] urls = {"/home", "/cart", "/pay", "/info?id?"};
            Random random = new Random();
            while (ifRun) {
                //直接返回一个对象
                ctx.collect(new Event(users[random.nextInt(users.length)],
                        urls[random.nextInt(urls.length)],
                        Calendar.getInstance().getTimeInMillis()));
                //1s  生成一条
                Thread.sleep(1000);
            }

        }


        @Override
        public void cancel() {
            //取消
            ifRun = false;
        }
    }

}
