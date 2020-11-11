package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MostTreeMapper extends Mapper<Object,Text,Text,IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        if(!value.toString().contains("ESPECE")){
            Text district = new Text(value.toString().split(";")[1]);
            context.write(district, one);
        }
    }
}
