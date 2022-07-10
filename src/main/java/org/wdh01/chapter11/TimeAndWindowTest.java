package org.wdh01.chapter11;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.wdh01.bean.Event;
import org.wdh01.chapter05.ClickSource;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class TimeAndWindowTest {
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
//流转换表时 定义时间属性
        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );
        // 流 转换表
        Table clickTable = tableEnv.fromDataStream(eventStream, $("user"), $("url"), $("timestamp").as("ts")
                , $("et").rowtime());
        //查看表结构
        clickTable.printSchema();

        //聚合查询转换
       /* Table aggTable = tableEnv.sqlQuery("select user_name,count(1) from clickTable group by user_name");

        tableEnv.toChangelogStream(aggTable).print(" agg user cnt");
*/
        //分组窗口聚合

        Table tumbtable = tableEnv.sqlQuery("select " +
                " user_name,count(1) as cnt, " +
                " TUMBLE_END(et,INTERVAL '2' SECOND) AS entT " +
                " from clickTable " +
                " group by " +
                " user_name,TUMBLE(et,INTERVAL '2' SECOND) ");
       // tableEnv.toChangelogStream(tumbtable).print(" TUMBLE cnt");
        //窗口聚合
        //1、滚动窗口
        Table tvtable = tableEnv.sqlQuery(" select user_name,COUNT(1) as cnt, " +
                " window_end as endT " +
                " from TABLE( " +
                " TUMBLE(TABLE clickTable,DESCRIPTOR(et),INTERVAL '2' SECOND) " +
                " ) " +
                " GROUP BY user_name,window_end,window_start ");
        tableEnv.toChangelogStream(tvtable).print(" tvtable cnt");

        //2、滑动窗口
        Table slidtable = tableEnv.sqlQuery(" select user_name,COUNT(1) as cnt, " +
                " window_end as endT " +
                " from TABLE( " +
                " HOP(TABLE clickTable,DESCRIPTOR(et),INTERVAL '2' SECOND,INTERVAL '4' SECOND) " +
                " ) " +
                " GROUP BY user_name,window_end,window_start ");

        //tableEnv.toChangelogStream(slidtable).print(" slidtable cnt");
        //3、累计窗口
        //2、滑动窗口
        Table leijtable = tableEnv.sqlQuery(" select user_name,COUNT(1) as cnt, " +
                " window_end as endT " +
                " from TABLE( " +
                " CUMULATE(TABLE clickTable,DESCRIPTOR(et),INTERVAL '2' SECOND,INTERVAL '4' SECOND) " +
                " ) " +
                " GROUP BY user_name,window_end,window_start ");

        //tableEnv.toChangelogStream(leijtable).print(" leijtable cnt");
        //开窗聚合
        Table overtable = tableEnv.sqlQuery(" select user_name, " +
                " avg(ts) OVER ( PARTITION BY  user_name ORDER BY et " +
                " ROWS BETWEEN 3 PRECEDING AND CURRENT ROW )  as avg_ts from clickTable ");
        tableEnv.toChangelogStream(overtable).print(" overtable cnt");
        env.execute();
    }
}
