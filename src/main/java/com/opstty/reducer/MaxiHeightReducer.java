package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MaxiHeightReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable maxH = new IntWritable();
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int max = 0;
        for (IntWritable val : values) {
            if(max < val.get()) {
                max = val.get();
            }
        }
        maxH.set(max);
        context.write(key, maxH);
    }
}
