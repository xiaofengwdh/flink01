package org.wdh01.chapter05;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * 自定义source
 */
public class SourceCustmomParallTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(2);
        DataStreamSource<Integer> eventSource = environment.addSource(new ParallelCustomSource());

        eventSource.print();
        environment.execute();
    }

    /**
     * 自定义并行 SourceFunction
     */
    public static class ParallelCustomSource implements ParallelSourceFunction<Integer> {
        // 声明标志位
        private Boolean running = true;
        Random random = new Random();

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                //搜集数据
                ctx.collect(random.nextInt());
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}


