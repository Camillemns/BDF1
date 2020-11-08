package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortHeightMapper extends Mapper<Object, Text, Text, NullWritable> {
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        if(!value.toString().contains("ESPECE")){
            Text id = new Text(value.toString().split(";")[11]);
            Text height_str = new Text(value.toString().split(";")[6]);
            if(!height_str.toString().isEmpty()) {
                IntWritable val = new IntWritable((int)Float.parseFloat(height_str.toString()));
                context.write(val, id);
            }
        }
    }
}
