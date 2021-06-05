package com;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FourthJob {
    public static void mainJob() {
        try {
            Job job = Job.getInstance(new Configuration());
            job.setJarByClass(FourthJob.class);
            //设置任务的输出key类型，value类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setMapperClass(FourthMapper.class);
            job.setCombinerClass(FourthReducer.class);
            job.setReducerClass(FourthReducer.class);
            //设置任务运行时，数据的输入输出目录，这里的输入数据是上一个mapreduce的输出
            FileInputFormat.addInputPath(job, new Path(Paths.TJ_OUTPUT3));
            FileOutputFormat.setOutputPath(job, new Path(Paths.TJ_OUTPUT4));
            if (job.waitForCompletion(true)) {
                System.out.println("com.FourthJob-执行完毕");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
