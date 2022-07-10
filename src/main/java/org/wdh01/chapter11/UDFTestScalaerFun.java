package org.wdh01.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

public class UDFTestScalaerFun {
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
        tableEnv.createTemporarySystemFunction("MyHash", MyHashFun.class);

        //调用函数进行查询转换
        Table table = tableEnv.sqlQuery("select user_name,MyHash(user_name) from clickTable");

        //输出结果
        tableEnv.toDataStream(table).print();

        env.execute();
    }

    //自定义实现scalaFun
    public static class MyHashFun extends ScalarFunction {
        public int eval(String str) {
            return str.hashCode();
        }
    }
}
