package org.wdh01.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import sun.java2d.loops.ProcessPath;

import java.util.Properties;

/**
 * 批处理 wordcount
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        //1、创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2、读取数据
        DataSource<String> lineDS = env.readTextFile("input/word.txt");
        //lineDS 分词&转换结构
        FlatMapOperator<String, Tuple2<String, Long>> wordOne = lineDS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            //分词
            String[] words = line.split(" ");
            //包装元组
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //按照第一个元素分组
        UnsortedGrouping<Tuple2<String, Long>> groupDate = wordOne.groupBy(0);
        //聚合
        AggregateOperator<Tuple2<String, Long>> sum = groupDate.sum(1);





        //输出结果
        sum.print();
    }
}
