package com;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/**
 * 主要用于计算tf-idf
 * 输出为1个文件，目录为TJ_OUTPUT3
 */
public class ThirdJob {
  
    public static void mainJob() {  

        try {  
            Job job = Job.getInstance(new Configuration());  
            job.setJarByClass(ThirdJob.class);

            //setup函数的输入
            //第一个job的part-r-00003（微博总条数）
            //第二个job的part-r-00000（每个词的DF）
            job.addCacheFile(new Path(Paths.TJ_OUTPUT1 + "/part-r-00003").toUri());
            job.addCacheFile(new Path(Paths.TJ_OUTPUT2 + "/part-r-00000").toUri());
            //设置任务的输出key类型，value类型
            job.setOutputKeyClass(Text.class);  
            job.setOutputValueClass(Text.class);

            job.setMapperClass(ThirdMapper.class);
            job.setCombinerClass(ThirdReducer.class);
            job.setReducerClass(ThirdReducer.class);
            //map函数的输入
            //设置任务运行时，数据的输入输出目录，这里的输入数据是第一个job的输出，存放着每条微博中单词的TF
            FileInputFormat.addInputPath(job, new Path(Paths.TJ_OUTPUT1));
            FileOutputFormat.setOutputPath(job, new Path(Paths.TJ_OUTPUT3));  
            if (job.waitForCompletion(true)) {  
                System.out.println("com.Third-执行完毕");
                //下一个任务FourthJob
                FourthJob.mainJob();
            }  
        } catch (Exception e) {  
            e.printStackTrace();  
        }  
    }  
}  