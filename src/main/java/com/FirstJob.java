package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 主要用于统计TF
 * 输出为4个文件，目录为TJ_OUTPUT1
 * 因为设置了reduce数目为4
 * part-r-00000～000002为统计的word_id的频率（该id的评论中，word的TF）
 * part-r-00003为<count,微博总条数>
 */

public class FirstJob {
    public static void main(String[] args) {
        try {
            Job job = Job.getInstance(new Configuration());
            job.setJarByClass(FirstJob.class);
            //设置任务的输出key类型，value类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            //设置reduce个数为4
            job.setNumReduceTasks(4);
            //定义一个partition表分区，哪些数据应该进入哪些分区
            job.setPartitionerClass(FirstPartition.class);
            job.setMapperClass(FirstMapper.class);
            job.setCombinerClass(FirstReducer.class);
            job.setReducerClass(FirstReducer.class);
            //设置执行任务时，数据获取的目录及数据输出的目录
            FileInputFormat.addInputPath(job, new Path(Paths.TJ_INPUT));
            FileOutputFormat.setOutputPath(job, new Path(Paths.TJ_OUTPUT1));
            if (job.waitForCompletion(true)) {
                System.out.println("com.FirstJob-执行完毕");
                //下一个任务SecondJob
                SecondJob.mainJob();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
