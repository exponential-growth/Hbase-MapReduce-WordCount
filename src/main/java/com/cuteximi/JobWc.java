package com.cuteximi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @program: wchbase
 * @description: 单词统计任务
 * @author: TSL
 * @create: 2017-10-31 18:51
 **/
public class JobWc {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 设置任务: HDFS、Yarn 资源调度、zookeeper
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node04:8020");
        configuration.set("yarn.resourcemanager.hostname","node01:8088");
        configuration.set("hbase.zookeeper.quorum","node01,node02,node03");

        // 创建任务
        Job job = Job.getInstance(configuration);

        // 设置任务的执行主类
        job.setJarByClass(JobWc.class);

        // 设置执行 Map 的类
        job.setMapperClass(MapWc.class);

        // 设置map阶段的输出 key value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置输入的任务和路径。Path 为 hadoop.fs.Path
        FileInputFormat.addInputPath(job,new Path("/input/wc"));

        // 与原始的单词统计不同
        TableMapReduceUtil.initTableReducerJob("wc",ReduceWc.class,job);


        if(job.waitForCompletion(true)){
            System.out.println("任务成功！！！！");
        }else{
            System.out.println("任务失败！！！");
        }

    }

}
