package com.opstty.job;

import com.opstty.Informations;
import com.opstty.mapper.MaxiHeightMapper;
import com.opstty.mapper.OldestTreeMapper;
import com.opstty.reducer.MaxiHeightReducer;
import com.opstty.reducer.OldestTreeReducer;
import org.apache.commons.lang.ObjectUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class OldestTree {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: oldestTree <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "oldestTree");
        job.setJarByClass(OldestTree.class);
        job.setMapperClass(OldestTreeMapper.class);
        job.setReducerClass(OldestTreeReducer.class);
        // for mapper
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Informations.class);
        // for reducer
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
