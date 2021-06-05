package com;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


/**
 * 划分键值空间（key space）
 * 中间过程的key应该分给4个reduce任务中的哪一个来进行reduce操作。
 */
public class FirstPartition extends HashPartitioner<Text, IntWritable> {

    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        //如果key值为count，就返回3（第四个分区），
        if (key.equals(new Text("count"))) {
            return 3;
        }
        //其他的key值就平均分配到三个分区，
        else {
            //numReduceTasks - 1的意思是有4个reduce，其中一个已经被key值为count的占用了，所以数据只能分配到剩下的三个分区中了
            //使用super，可以调用父类的HashPartitioner
            return super.getPartition(key, value, numReduceTasks - 1);
        }
    }

}
