package com.opstty.job;

import com.opstty.Informations;
import com.opstty.mapper.MostTreeMapper;
import com.opstty.mapper.MostTreeSecondMapper;
import com.opstty.reducer.MostTreeReducer;
import com.opstty.reducer.MostTreeSecondReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MostTree {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: mostTree <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "mostTree");
        job.setJarByClass(MostTree.class);
        job.setMapperClass(MostTreeMapper.class);
        job.setCombinerClass(MostTreeReducer.class);
        job.setReducerClass(MostTreeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1] + "/temp"));
        job.waitForCompletion(true);
        Job job2 = Job.getInstance(conf);
        job2.setJarByClass(MostTree.class);
        job2.setMapperClass(MostTreeSecondMapper.class);
        job2.setReducerClass(MostTreeSecondReducer.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Informations.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job2, new Path(otherArgs[otherArgs.length - 1] + "/temp"));
        }
        FileOutputFormat.setOutputPath(job2,
                new Path(otherArgs[otherArgs.length - 1] +"/final"));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
