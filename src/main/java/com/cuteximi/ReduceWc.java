package com.cuteximi;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @program: wchbase
 * @description: reduce 任务
 * @author: TSL
 * @create: 2017-10-31 18:51
 **/
// 注意，这里继承的类变了，需要的参数也变了。。。。
public class ReduceWc extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int num = 0;
        for(IntWritable i :values){
            num += i.get();
        }

        System.out.println("============"+key.toString()+":=========="+num+"个");

        Put put = new Put(key.toString().getBytes());

        // 添加到 Hbase
        put.add("cf".getBytes(),"count".getBytes(),(num+"").getBytes());

        // 输出
        context.write(null,put);

    }
}
