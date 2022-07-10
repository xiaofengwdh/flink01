package org.wdh01.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.wdh01.bean.Event;
import org.wdh01.bean.UrlViewCount;
import org.wdh01.chapter05.ClickSource;
import org.wdh01.chapter06.UrlCountViewExample;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class TopNExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> eventtStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        //url 分组，统计窗口内 url 访问量

        SingleOutputStreamOperator<UrlViewCount> urlCountStream = eventtStream.keyBy(data -> data.url)
                // 窗口大小 10s，滑动步长 5s
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlCountViewExample.UrlViewCountAgg(),
                        new UrlCountViewExample.UrlViewCountResult());

        urlCountStream.print("  url Count ");
        //同一窗口访问量收集排序 取top2
        urlCountStream.keyBy(data -> data.end)
                .process(new TopNProcessResult(2))
                .print();

        env.execute();
    }

    public static class TopNProcessResult extends KeyedProcessFunction<Long, UrlViewCount, String> {
        private Integer n;

        public TopNProcessResult(Integer n) {
            this.n = n;
        }

        //定义列表状态
        private ListState<UrlViewCount> urlViewCountListState;

        //运行时环境 获取状态
        @Override
        public void open(Configuration parameters) throws Exception {
            urlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-count-list", Types.POJO(UrlViewCount.class))
            );
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            //将数据保存到 ctx
            urlViewCountListState.add(value);
            //注册定时器
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey() + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCount> urlViewCountsArrayList = new ArrayList<>();

            for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
                urlViewCountsArrayList.add(urlViewCount);
            }
            //排序
            urlViewCountsArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.cnt.intValue() - o1.cnt.intValue();
                }
            });

            //包装
            StringBuilder result = new StringBuilder();

            result.append("-----------------------------\n");
            result.append("窗口结束时间：" + new Timestamp(ctx.getCurrentKey()) + "\n");
            //取 list 钱个
            for (int i = 0; i < 2; i++) {
                UrlViewCount cuurTuple = urlViewCountsArrayList.get(i);
                String info = "No" + (i + 1) + " "
                        + "url" + cuurTuple.url + " "
                        + "访问量 " + cuurTuple.cnt + " \n";
                result.append(info);
                result.append("-----------------------------\n");
                out.collect(result.toString());
            }

        }
    }
}
