package com;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 主要用于统计TF
 * 将一个key（word_id）关联的一组中间数值(count=1)集归约(求和)
 * 输出：
 * k：word_id
 * v：count（对于该id的评论，word的TF=count）
 */
public class FirstReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    //text就是map中计算出来的key值(即word_id)
    protected void reduce(Text text, Iterable<IntWritable> iterable, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable intWritable : iterable) {
            sum += intWritable.get();
        }
        //计算微博总条数
        if (text.equals("count")) {
            System.out.println(text.toString() + "==" + sum);
        }
        //计算word_id的个数
        context.write(text, new IntWritable(sum));
    }
}
