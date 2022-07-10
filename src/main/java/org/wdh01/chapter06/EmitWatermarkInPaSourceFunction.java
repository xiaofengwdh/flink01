package org.wdh01.chapter06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.wdh01.chapter05.ClickSourceWithWatermark;

public class EmitWatermarkInPaSourceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSourceWithWatermark()).print();
        env.execute();

    }
}
