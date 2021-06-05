package com;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;


/**
 * 输出：
 * k：id
 * v：word：tf-idf
 */
public class ThirdMapper extends Mapper<LongWritable, Text, Text, Text> {
    public static Map<String, Integer> cmap = null; //《count，微博总条数》
    public static Map<String, Integer> df = null;   //《word，DF》
    //setup方法，在map之前调用
    protected void setup(Context context) throws IOException, InterruptedException {  
        if (cmap == null || cmap.size() == 0 || df == null || df.size() == 0) {  
            //从cachefile中读取文件
            //文件有两个为    第一个job的part-r-00003（微博总条数）
            //              第二个job的part-r-00000（每个词的DF）
            URI[] ss = context.getCacheFiles();  
            if (ss != null) {  
                for (int i = 0; i < ss.length; i++) {  
                    URI uri = ss[i];  
                    //判断如果该文件是part-r-00003，
                    // 那就是count文件，将数据取出来放入到一个cmap，所以cmap的第一个数据为《count，微博总条数》
                    if (uri.getPath().endsWith("part-r-00003")) {  
                        Path path = new Path(uri.getPath());  
                        BufferedReader br = new BufferedReader(new FileReader(path.getName()));  
                        String line = br.readLine();  
                        if (line.startsWith("count")) {  
                            String[] ls = line.split("\t");  
                            cmap = new HashMap<String, Integer>();
                            //cmap这个map的第一个数据以"count"作为key，以微博的总条数值作为value 《count，微博总条数》
                            cmap.put(ls[0], Integer.parseInt(ls[1].trim()));  
                        }  
                        br.close();  
                    } else {  
                    //其他的认为是DF文件（即第二个job的part-r-000000），将数据取出来放到df中
                        df = new HashMap<String, Integer>();  
                        Path path = new Path(uri.getPath());  
                        BufferedReader br = new BufferedReader(new FileReader(path.getName()));  
                        String line;  
                        while ((line = br.readLine()) != null) {  
                            String[] ls = line.split("\t");
                            //df这个map以单词作为key，以单词的df值作为value
                            df.put(ls[0], Integer.parseInt(ls[1].trim()));
                        }  
                        br.close();  
                    }  
                }  
            }  
        }  
    }  
  
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //从FileInputFormat中读取文件
        //文件有4个，为第一个job的输出文件，即part-r-00000～000003
        //其中part-r-00003在map函数中没用
        FileSplit fs = (FileSplit) context.getInputSplit();  
  
        if (!fs.getPath().getName().contains("part-r-00003")) {
            String[] v = value.toString().trim().split("\t");  
            if (v.length >= 2) {
                //v[0]为：word_id
                //v[1]为：tf
                int tf = Integer.parseInt(v[1].trim());
                String[] ss = v[0].split("_");  
                if (ss.length >= 2) {
                    //ss[0]为word
                    //ss[1]为id
                    String w = ss[0];
                    String id = ss[1];  
                    //执行W = TF * Log(N/DF)计算  
                    double s = tf * Math.log(cmap.get("count") / df.get(w));  
                    //格式化，保留小数点后五位  
                    NumberFormat nf = NumberFormat.getInstance();  
                    nf.setMaximumFractionDigits(5);  
                    //以 微博id+词：权重 输出  
                    context.write(new Text(id), new Text(w + ":" + nf.format(s)));  
                }  
            } else {  
                System.out.println(value.toString() + "-------------");  
            }  
        }  
    }  
}  