package org.wdh01.chapter11;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;

public class UDFTest_TableFunction {
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
        tableEnv.createTemporarySystemFunction("MYSplit", MYSplit.class);

        //调用函数进行查询转换
        Table table = tableEnv.sqlQuery("select user_name,url,word,length from clickTable" +
                ", LATERAL TABLE(MYSplit(url)) as T(word,length) ");

        //输出结果
        tableEnv.toDataStream(table).print();

        env.execute();
    }

    //自定义表函数
    public static class MYSplit extends TableFunction<Tuple2<String, Integer>> {
        public void eval(String s) {
            String[] split = s.split("\\?");
            for (String s1 : split) {
                collect(Tuple2.of(s1, s1.length()));
            }
        }

    }
}
