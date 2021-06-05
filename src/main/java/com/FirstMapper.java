package com;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;
import java.io.IOException;
import java.io.StringReader;

/**
 * 输出：
 * k：word_id
 * v：1
 * 其中<count,1>的个数等于微博条数
 */

public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //一次读入一行
        //line[0]为：微博ID，line[1]为：微博内容
        String[] line = value.toString().split("\t");  //以tab键为分隔符
        if (line.length >= 2) {
            //微博的ID
            String id = line[0].trim();
            //微博的内容
            String content = line[1].trim();
            //给内容进行分词
            StringReader sr = new StringReader(content);
            IKSegmenter iks = new IKSegmenter(sr,true);
            Lexeme lexeme = null;
            while ((lexeme = iks.next()) != null) {
                //word就是分完的每个词
                String word = lexeme.getLexemeText();
                context.write(new Text(word + "_" + id), new IntWritable(1));
            }
            sr.close();
            //最后加入一个key为count的键值对，value也为1，以便在reducer中统计微博条数
            context.write(new Text("count"), new IntWritable(1));
        } else {
            System.err.println("error:" + value.toString() + "-----------------------");
        }
    }
}
