package com.opstty.reducer;

import com.opstty.Informations;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.sound.sampled.Line;
import java.io.IOException;

public class OldestTreeReducer extends Reducer<IntWritable, Informations,Text,NullWritable> {
    public void reduce(IntWritable key, Iterable<Informations> values, Context context) throws IOException, InterruptedException {
        Text older = new Text();
        int year = 2020;
        for(Informations val : values) {
                if(val.getAge().get() < year) {
                    older.set(val.getDistrict());
                    year = Integer.parseInt(val.getAge().toString());
                }
        }
        context.write(older, NullWritable.get());
    }
}
