package org.wdh01.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TopNExample {
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

        //普通 Topn ，浏览量对打的2数据
        Table topNtable = tableEnv.sqlQuery(" select user_name,cnt,rn " +
                " from (" +
                " select *,row_number() over( order by cnt desc) as rn" +
                " from  (select user_name,count(url) as cnt from clickTable group by user_name ) " +
                ") where rn<=2 ");

        // tableEnv.toChangelogStream(topNtable).print();

        //窗口TopN:统计一段时间内活跃用户


        Table windowtopNtable = tableEnv.sqlQuery(" select user_name,cnt,rn " +
                " from (" +
                " select *,row_number() over(partition by window_start,window_end order by cnt desc) as rn " +
                " from  (select user_name,count(url) as cnt," +
                "" +
                "window_start,window_end " +
                " from TABLE(TUMBLE(TABLE clickTable,DESCRIPTOR(et),INTERVAL '5' SECOND)) " +
                " group by user_name, window_start,window_end ) " +
                ") where rn<=2 ");
        tableEnv.toDataStream(windowtopNtable).print();

        env.execute();
    }
}
