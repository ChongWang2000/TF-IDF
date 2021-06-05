package com;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 主要用于计算tf-idf，并整合每个评论的所有word
 * 输出：
 * k：id
 * v：{word:tf-idf}  中间以"\t"作为分隔符
 */
public class ThirdReducer extends Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> arg1, Context context) throws IOException, InterruptedException {

        StringBuffer sb = new StringBuffer();

        for (Text i : arg1) {
            sb.append(i.toString() + "\t");
        }

        context.write(key, new Text(sb.toString()));
    }

}
