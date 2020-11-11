package com.opstty.reducer;

import com.opstty.Informations;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MostTreeSecondReducer extends Reducer<NullWritable, Informations, Text,NullWritable> {
    Text result = new Text();
    public void reduce(NullWritable key, Iterable<Informations> values, Context context)
            throws IOException, InterruptedException {
        int max = 0;
        Text district = new Text();
        for (Informations val : values) {
            if(Integer.parseInt(val.getAge().toString()) >= max) {
                max = Integer.parseInt(val.getAge().toString());
                district.set(val.getDistrict());
            }
        }
        result.set(district.toString());
        context.write(district, NullWritable.get());
    }
}
