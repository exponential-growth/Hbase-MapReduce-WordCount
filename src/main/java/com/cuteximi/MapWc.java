package com.cuteximi;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @program: wchbase
 * @description: map 任务
 * @author: TSL
 * @create: 2017-10-31 18:51
 **/
public class MapWc extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拿到每一行的值，并且转为 String 字符串
        String lines = value.toString();
        // 切分单词
        StringTokenizer words = new StringTokenizer(lines);
        // 遍历
        while(words.hasMoreElements()){
            // 往外写数据
            context.write(new Text(words.nextToken()),new IntWritable(1));
        }
    }
}
