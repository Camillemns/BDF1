package com.opstty.mapper;

import com.opstty.Informations;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MostTreeSecondMapper extends Mapper<Object, Text, NullWritable, Informations> {
    public void map(Object key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String [] input = value.toString().split("\t");
        Text district = new Text(input[0]);
        IntWritable number = new IntWritable(Integer.parseInt(input[1]));
        Informations i = new Informations(district,number);
        context.write(NullWritable.get(),i);
    }
}
