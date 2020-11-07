package com.opstty.job;

import com.opstty.mapper.DistrictTreeMapper;
import com.opstty.mapper.NumberSpeciesMapper;
import com.opstty.mapper.SpeciesMapper;
import com.opstty.reducer.DistrictTreeReducer;
import com.opstty.reducer.NumberSpeciesReducer;
import com.opstty.reducer.SpeciesReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NumberSpecies {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: numberSpe <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "numberSpe");
        job.setJarByClass(NumberSpecies.class);
        job.setMapperClass(NumberSpeciesMapper.class);
        job.setCombinerClass(NumberSpeciesReducer.class);
        job.setReducerClass(NumberSpeciesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}