package org.wdh01.chapter11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class CommonAPITest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //创建 tableEnv 方式1 通过流处理环境创建
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建 tableEnv 方式2 通过配置信息创建
/*        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv2 = TableEnvironment.create(settings);*/

        //创建表
        String createDDl = "CREATE TABLE clicksTbale (" +
                " user_name STRING," +
                " url STRING ," +
                " ts BIGINT ) WITH (" +
                " 'connector' =  'filesystem' ," +
                " 'path' = 'input/clicks.txt' ," +
                " 'format' = 'csv' )";
        tableEnv.executeSql(createDDl);


        //调用APi 进行查询转换
        Table clicksTable = tableEnv.from("clicksTbale");
        //查询
        Table select = clicksTable.where($("user_name").isEqual("依琳"))
                .select($("user_name"), $("url"));
        //注册查询结果为表对象
        tableEnv.createTemporaryView("res", select);

        //执行查询sql进行表查询转换
        Table table = tableEnv.sqlQuery("select url,user_name from res");

        Table aggres = tableEnv.sqlQuery("select user_name,count(url) as cnt from  clicksTbale group by user_name");


        //创建用于输出的表
        String outDDl = "CREATE TABLE outTbale (" +
                " user_name STRING," +
                " url STRING " +
                "  ) WITH (" +
                " 'connector' ='filesystem' , " +
                "'path' = 'output' ," +
                " 'format' = 'csv' )";
        tableEnv.executeSql(outDDl);

       //输出
        table.executeInsert("outTbale");

       //创建一个用于控制台打印输出的表

        //创建用于输出的表
        String outDDl1 = "CREATE TABLE outTbale1 (" +
                " user_name STRING," +
                " url STRING " +
                "  ) WITH (" +
                " 'connector' ='print' )";
        tableEnv.executeSql(outDDl1);


        //创建用于输出的表
        String outagg = "CREATE TABLE outTbalea (" +
                " user_name STRING," +
                " cnt BIGINT " +
                "  ) WITH (" +
                " 'connector' ='print' )";
         tableEnv.executeSql(outagg);

        //select.executeInsert("outTbale1");
        aggres.executeInsert("outTbalea");
    }
}
