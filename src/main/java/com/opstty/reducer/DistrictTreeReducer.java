package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DistrictTreeReducer extends Reducer<Text, NullWritable, Text, NullWritable>{

    public void reduce(Text arrondissement, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        context.write(arrondissement, NullWritable.get());
    }
}
