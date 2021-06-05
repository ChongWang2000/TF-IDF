package com;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;


/**
 * 输出：
 * k：word
 * v：1
 */
public class SecondMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //输入为FirstJob的输出part-r-00000~00002
        FileSplit fs = (FileSplit) context.getInputSplit();
        //map时拿到split片段所在文件的文件名(不包括part-r-00003)
        if (!fs.getPath().getName().contains("part-r-00003")) {
            //line[0]为：word_id，   line[1]为：词频
            String[] line = value.toString().trim().split("\t");
            if (line.length >= 2) {
                //ss[0]为：word，ss[1]为：微博id
                String[] ss = line[0].split("_");
                if (ss.length >= 2) {
                    //从word_id中拿到word
                    String w = ss[0];
                    //统计DF，该词在所有微博中出现的条数，一条微博即使出现两次该词，也算一条
                    context.write(new Text(w), new IntWritable(1));
                }
            } else {
                System.out.println("error:" + value.toString() + "-------------");
            }
        }
    }
}
