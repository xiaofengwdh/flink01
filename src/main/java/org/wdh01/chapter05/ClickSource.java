package org.wdh01.chapter05;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.wdh01.bean.Event;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    // 声明标志位
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        //随机生成数据
        Random random = new Random();
        //随机范围
        String[] users = {"令狐冲", "依琳", "任盈盈", "莫大", "风清扬"};

        String[] urls = {"./home", "./cat", "./pay", "./info"};
        //循环生成数据
        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            long timeInMillis = Calendar.getInstance().getTimeInMillis();
            ctx.collect(new Event(user, url, timeInMillis));
            //1s 生成一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
