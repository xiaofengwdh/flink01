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
import org.apache.hadoop.yarn.util.Times;
import org.wdh01.bean.Event;
import org.wdh01.bean.UrlViewCount;
import org.wdh01.chapter05.ClickSource;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class KeyedProcessTopN0701 {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //获取输入数据提取水位线
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                }));

        //url 分组
        SingleOutputStreamOperator<UrlViewCount> urlCntStream = eventStream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new AggregateFunction<Event, Long, Long>() {
                               //实现 AggregateFunction 进行增量聚合
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
                               //使用全窗口函数包装聚合结果
                               @Override
                               public void process(String url, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {

                                   long start = context.window().getStart();
                                   long end = context.window().getEnd();
                                   //封装输出结果
                                   out.collect(new UrlViewCount(url, elements.iterator().next(), start, end));
                               }
                           }
                );


        urlCntStream.keyBy(data -> data.end)
               .process(new KeyedProcessFunction<Long, UrlViewCount, String>() {

                    //定义属性
                    private Integer n;
                    //定义列表
                    private ListState<UrlViewCount> listState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //获取列表句柄
                        ListState<UrlViewCount> listState = getRuntimeContext().
                                getListState(new ListStateDescriptor<UrlViewCount>("url-view", Types.POJO(UrlViewCount.class)));
                    }

                    @Override
                    public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
                 //将 conut 数据添加到列表状态中
                        listState.add(value);
                        //注册一个 end + 1ms 的定时器，等到所有数据到齐排序
                        ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey()+1);

                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                      //将数据从列表状态中取出，写入arrayList 进行排序
                        ArrayList<UrlViewCount> urlViewCounts = new ArrayList<>();
                        for (UrlViewCount urlViewCount : listState.get()) {
                            urlViewCounts.add(urlViewCount);
                        }
                        //清空状态，。释放资源
                        urlViewCounts.clear();
                        //排序
                        urlViewCounts.sort(new Comparator<UrlViewCount>() {
                            @Override
                            public int compare(UrlViewCount o1, UrlViewCount o2) {
                                return o2.cnt.intValue()-o1.cnt.intValue();
                            }
                        });

                        //取前2名 排序
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.append("-------------------------\n");
                        stringBuilder.append("窗口结束时间 " + new Timestamp(timestamp-1) + "\n");
                        for (int i = 0; i < this.n; i++) {
                            UrlViewCount urlViewCount = urlViewCounts.get(i);
                            String info="No. " + (i+1) +" "
                                    +"url: " + urlViewCount.url+ " "
                                    +"浏览量 " +urlViewCount.cnt+"\n";
                            stringBuilder.append(info);
                        }

                        stringBuilder.append("--------------------------------");
                        out.collect(stringBuilder.toString());
                    }
                }).print();

        env.execute();
    }
}
