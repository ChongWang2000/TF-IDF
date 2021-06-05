package com;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * 主要用于统计DF
 * 输出：
 * k：word
 * v：count（对于所有评论，每个word的DF）
 */
public class SecondReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    protected void reduce(Text key, Iterable<IntWritable> arg1, Context context) throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable i : arg1) {
            sum = sum + i.get();
        }

        context.write(key, new IntWritable(sum));
    }

}

