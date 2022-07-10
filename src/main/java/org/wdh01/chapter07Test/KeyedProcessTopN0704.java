package org.wdh01.chapter07Test;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;
import org.wdh01.bean.UrlViewCount;
import org.wdh01.chapter05.ClickSource;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class KeyedProcessTopN0704 {
    public static void main(String[] args) throws Exception {
        //获取执行环境&设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        /**
         * 读取数据&提取水位线
         */
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        // 需要按照url分组，求出每个url的访问量
        SingleOutputStreamOperator<UrlViewCount> aggregateStream = eventStream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<Event, Long, Long>() {
                               @Override
                               public Long createAccumulator() {
                                   return 0L;
                               }

                               @Override
                               public Long add(Event value, Long accumulator) {
                                   return accumulator + 1L;
                               }

                               @Override
                               public Long getResult(Long accumulator) {
                                   return accumulator;
                               }

                               @Override
                               public Long merge(Long a, Long b) {
                                   return a + b;
                               }
                           }, new ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>() {

                               //包装数据
                               @Override
                               public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
                                   long start = context.window().getStart();
                                   long end = context.window().getEnd();
                                   //输出包装信息
                                   out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
                               }
                           }
                );

        aggregateStream.keyBy(data -> data.end)
                .process(new TopN(2)).print("res");


        env.execute();
    }

    public static class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {
        //入参
        private Integer n;
        //定义列表状态
        private ListState<UrlViewCount> listState;

        public TopN(Integer n) {
            this.n = n;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //获取列表状态
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>(" url-view-cnt-list", Types.POJO(UrlViewCount.class))
            );
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            //将 cnt 数据保存在 列表状态中
            listState.add(value);
            //注册一个window end+1s 定时器，等待所有数据到齐开始排序
            ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {

            //从列表状态中获取数据
            ArrayList<UrlViewCount> arrayList = new ArrayList<>();
            for (UrlViewCount urlViewCount : listState.get()) {
                arrayList.add(urlViewCount);
            }
            //清空状态释放资源
            listState.clear();
            //排序
            arrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.cnt.intValue() - o1.cnt.intValue();
                }
            });
            //取前3名，包装输出信息
            StringBuilder res = new StringBuilder();
            res.append("------------------------");
            res.append("窗口结束时间 " + new Timestamp(timestamp - 1) + "\n");
            for (Integer i = 0; i < this.n; i++) {
                UrlViewCount urlViewCount = arrayList.get(i);
                String info = "No. " + (i + 1) + " "
                        + "url: " + urlViewCount.url + " "
                        + "浏览量：" + urlViewCount.cnt + "\n";
                res.append(info);
            }
            res.append("------------------------");
            out.collect(res.toString());
        }
    }
}