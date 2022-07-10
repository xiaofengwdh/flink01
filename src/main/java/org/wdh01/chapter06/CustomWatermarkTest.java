package org.wdh01.chapter06;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

/**
 * 自定义水位线
 */
public class CustomWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //生成周期性水位线
        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new MyWatermarkStrategy())
                .print();

        env.execute();
    }

    //自定义周期性生成水位线
    public static class MyWatermarkStrategy implements WatermarkStrategy<Event> {


        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event element, long recordTimestamp) {
                    return element.timestamp * 1000L;
                }
            };
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            //周期生成水位线
            // return new MyPeriodicGenerator();
            //断点生成水位线
            return new MyPunctuatedGenerator();
        }
    }

    //断点生成水位线
    public static class MyPunctuatedGenerator implements WatermarkGenerator<Event> {

        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            //只有遇到特定数据时，才发送水位线
            if (event.user.equals("依琳")) {
                output.emitWatermark(new Watermark(event.timestamp - 1L));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //onEvent 已经发送了水位线，onPeriodicEmit不做处理即可
        }
    }


    //周期性生成水位线
    public static class MyPeriodicGenerator implements WatermarkGenerator<Event> {
        //延迟时间
        private long delayTime = 5000L;
        //观察到最大时间cuo
        private long maxTs = Long.MIN_VALUE + delayTime + 1L;

        //每来一条数据调研一次
        @Override
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput output) {
            //更新最大时间cuo
            maxTs = Math.max(event.timestamp, maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //发射水位线，默认200 ms 调用一次
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }


}
