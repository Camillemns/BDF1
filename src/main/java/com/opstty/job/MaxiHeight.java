package com.opstty.job;

import com.opstty.mapper.MaxiHeightMapper;
import com.opstty.mapper.TokenizerMapper;
import com.opstty.reducer.IntSumReducer;
import com.opstty.reducer.MaxiHeightReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MaxiHeight {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: maxiHeight <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "maxiHeight");
        job.setJarByClass(MaxiHeight.class);
        job.setMapperClass(MaxiHeightMapper.class);
        job.setCombinerClass(MaxiHeightReducer.class);
        job.setReducerClass(MaxiHeightReducer.class);
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

