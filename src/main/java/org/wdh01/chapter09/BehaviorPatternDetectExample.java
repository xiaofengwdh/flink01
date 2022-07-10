package org.wdh01.chapter09;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BehaviorPatternDetectExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //行为数据
        DataStreamSource<Action> acctionStream = env.fromElements(
                new Action("001", "login"),
                new Action("001", "home"),
                new Action("002", "login"),
                new Action("002", "cart")
        );

        //行为模式 ：广播流
        DataStreamSource<Pattern> patternStream = env.fromElements(
                new Pattern("login", "home"),
                new Pattern("login", "cart")
        );
        //构建广播状态描述
        MapStateDescriptor<Void, Pattern> descriptor = new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class));
        //构建广播流
        BroadcastStream<Pattern> broadcastStream = patternStream.broadcast(descriptor);
//链接2条流
        SingleOutputStreamOperator<Tuple2<String, Pattern>> process = acctionStream.keyBy(data -> data.id)
                .connect(broadcastStream)
                .process(new PatternDetector());

        process.print();
        env.execute();
    }

    public static class PatternDetector extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {
        //定义kedStream,保存上次行为
        ValueState<String> prevActionState;

        @Override
        public void open(Configuration parameters) throws Exception {
            prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-action", String.class));
        }

        @Override
        public void processElement(Action value, ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
//从广播状态获取匹配规则
            ReadOnlyBroadcastState<Void, Pattern> pattern = ctx.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));
            Pattern pattern1 = pattern.get(null);

            //获取上次行为
            String value1 = prevActionState.value();
//判断是否匹配
            if (pattern1 != null && value1 != null) {
                if (pattern1.action1.equals(value1) && pattern1.action2.equals(value.action)) {
                    out.collect(Tuple2.of(ctx.getCurrentKey(),pattern1));
                }
            }
            //未匹配：状态更新
            prevActionState.update(value.action);

        }

        @Override
        public void processBroadcastElement(Pattern value, Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
            //从上下文中获取广播状态，并用当前数据更新状态
            BroadcastState<Void, Pattern> pattern = ctx.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));
            //更新状态
            pattern.put(null, value);
        }


    }


    public static class Action {
        public String id;
        public String action;

        public Action(String id, String action) {
            this.id = id;
            this.action = action;
        }

        public Action() {
        }

        @Override
        public String toString() {
            return "Action{" +
                    "id='" + id + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    public static class Pattern {
        public String action1;
        public String action2;

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        public Pattern() {
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }
}
