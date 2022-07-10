package org.wdh01.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

public class UDFTest_AggregateFunction {
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
        //注册自定义函数：标量函数
        tableEnv.createTemporarySystemFunction("myAggf", myAgg.class);

        //调用函数进行查询转换
        Table table = tableEnv.sqlQuery(" select user_name,myAggf(ts,1) as w_avg from clickTable group by user_name ");

        //输出结果
        tableEnv.toChangelogStream(table).print();

        env.execute();
    }

    //单独定义累加器类型
    public static class WeightAgg {
        public long sum = 0;
        public int count = 0;
    }

    public static class myAgg extends AggregateFunction<Long, WeightAgg> {

        @Override
        public Long getValue(WeightAgg weightAgg) {
            if (weightAgg.count == 0) {
                return 0L;
            } else {
                return weightAgg.sum / weightAgg.count;
            }

        }

        @Override
        public WeightAgg createAccumulator() {
            return new WeightAgg();
        }

        //累加计算
        public void accumulate(WeightAgg accumulate, Long ivalue, Integer iweight) {
            accumulate.sum += ivalue * iweight;
            accumulate.count += iweight;
        }
    }
}
