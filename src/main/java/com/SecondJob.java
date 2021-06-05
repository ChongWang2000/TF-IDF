package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 主要用于统计DF
 * 输出为1个文件，目录为TJ_OUTPUT2
 */
public class SecondJob {
  
    public static void mainJob() {
        try {  
            Job job = Job.getInstance(new Configuration());  
            job.setJarByClass(SecondJob.class);
            //设置任务的输出key类型，value类型
            job.setOutputKeyClass(Text.class);  
            job.setOutputValueClass(IntWritable.class);

            job.setMapperClass(SecondMapper.class);
            job.setCombinerClass(SecondReducer.class);
            job.setReducerClass(SecondReducer.class);
            //设置任务运行时，数据的输入输出目录，这里的输入数据是上一个mapreduce的输出  
            FileInputFormat.addInputPath(job, new Path(Paths.TJ_OUTPUT1));  
            FileOutputFormat.setOutputPath(job, new Path(Paths.TJ_OUTPUT2));  
            if (job.waitForCompletion(true)) {  
                System.out.println("com.SecondJob-执行完毕");
                //下一个任务 ThirdJob
                ThirdJob.mainJob();
            }  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
    }  
}  