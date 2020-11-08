package com.opstty.mapper;

import com.opstty.Informations;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import javax.sound.sampled.Line;
import java.io.IOException;

public class OldestTreeMapper extends Mapper<Object,Text,IntWritable, Informations> {
    IntWritable one = new IntWritable(1);
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if(!value.toString().contains("ESPECE")){
            Text district = new Text(value.toString().split(";")[1]);
            Text age_str = new Text(value.toString().split(";")[5]);
            if(!age_str.toString().isEmpty()) {
                IntWritable age = new IntWritable(Integer.parseInt(age_str.toString()));
                Informations couple = new Informations(district, age);
                context.write(one, couple);
            }
        }
    }
}
