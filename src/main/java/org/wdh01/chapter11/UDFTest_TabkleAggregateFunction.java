package org.wdh01.chapter11;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

public class UDFTest_TabkleAggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //创建表时定义时间属性
        String createDDl = "CREATE TABLE clickTable (" +
                " user_name STRING," +
                " url STRING ," +
                " ts BIGINT ," +
                "  et AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000))," +
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " +
                ") WITH (" +
                " 'connector' =  'filesystem' ," +
                " 'path' = 'input/clicks.txt' ," +
                " 'format' = 'csv' )";

        tableEnv.executeSql(createDDl);
        //调用函数进行查询转换
        Table table = tableEnv.sqlQuery(" select user_name,myAggf(ts,1) as w_avg from clickTable group by user_name ");

        //输出结果
        tableEnv.toChangelogStream(table).print();

        env.execute();
    }

    //定义累加器类型
    public static class TopAccumulate {
        public Long max;
        public Long secone;
    }

    //实现自定义表聚合
    public static class Top2 extends TableAggregateFunction<Tuple2<Long, Integer>, TopAccumulate> {

        @Override
        public TopAccumulate createAccumulator() {
            TopAccumulate topAccumulate = new TopAccumulate();
            topAccumulate.max = Long.MIN_VALUE;
            topAccumulate.secone = Long.MIN_VALUE;
            return topAccumulate;
        }

        //更新逻辑
        //累加计算
        public void accumulate(TopAccumulate accumulate, Long ivalue, Integer iweight) {
            if (ivalue > accumulate.max) {
                accumulate.secone = accumulate.max;
                accumulate.max = ivalue;
            } else if (ivalue > accumulate.secone) {
                accumulate.secone = ivalue;
            }
        }

        //输出
        public void emitValue(TopAccumulate accumulate, Collector<Tuple2<Long, Integer>> out) {
            if (accumulate.max != Long.MIN_VALUE) {
                out.collect(Tuple2.of(accumulate.max, 1));
            }
            if (accumulate.secone != Long.MIN_VALUE) {
                out.collect(Tuple2.of(accumulate.secone, 2));
            }
        }
    }
}
