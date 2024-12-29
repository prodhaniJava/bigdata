package com.prodhani.bigdata.mapreduce.country;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AgeCountryAnalysis extends Configured implements Tool  {

	@Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: AgeCountryAnalysis <input path> <output path>");
            return -1;
        }
        // Reuse the configuration passed from main
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Age Country Analysis");
        job.setJarByClass(AgeCountryAnalysis.class);
        job.setMapperClass(AgeCountryMapper.class);
        job.setReducerClass(AgeCountryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int res = ToolRunner.run(conf, new AgeCountryAnalysis(), args);
        System.exit(res);
    }
}
