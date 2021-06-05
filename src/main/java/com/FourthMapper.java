package com;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FourthMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        //line[0]为：word_id，
        //line[1~n]为word:tf-idf
        String[] line = value.toString().trim().split("\t");
        if (line.length >= 2) {
            String id=line[0];
            List<String> wordlist=new ArrayList<>();
            List<String> tfidflist=new ArrayList<>();
            for (int i = 1; i < line.length; i++) {
                if (line[i].contains(":")){
                    String[] ss=line[i].trim().split(":");
                    wordlist.add(ss[0]);
                    tfidflist.add(ss[1]);
                }
            }
            String[] words=wordlist.toArray(new String[wordlist.size()]);
            String[] tfidfs=tfidflist.toArray(new String[tfidflist.size()]);
            quicksort(words,tfidfs);

            StringBuilder res= new StringBuilder();
            for (int i = 0; i < words.length; i++) {
                res.append(words[i]).append(":").append(tfidfs[i]).append("\t");
            }

            context.write(new Text(id), new Text(String.valueOf(res)));

        } else {
            System.out.println("error:" + value.toString() + "-------------");
        }
    }


    public static void quicksort(String[] words, String[] tfidfs){
        System.out.println(words.length);
        System.out.println(tfidfs.length);
        qsort(words,tfidfs,0,words.length-1);
    }

    public static void qsort(String[] words, String[] tfidfs,int start,int end){
        if (start<end){
            int pivot=Partition(words,tfidfs,start,end);
            qsort(words,tfidfs,start,pivot-1);
            qsort(words,tfidfs,pivot+1,end);
        }
    }

    public static int Partition(String[] words, String[] tfidfs,int start,int end){
        Float pivot=Float.parseFloat(tfidfs[start]) ;
        String pivotword=words[start];
        while (start<end){
            while (start<end&&Float.parseFloat(tfidfs[end])<=pivot) end--;
            tfidfs[start]=tfidfs[end];
            words[start]=words[end];
            while (start<end&&Float.parseFloat(tfidfs[start])>=pivot) start++;
            tfidfs[end]=tfidfs[start];
            words[end]=words[start];
        }
        tfidfs[start]=pivot.toString();
        words[start]=pivotword;
        return start;
    }

}

