package com.prodhani.bigdata.mapreduce.country;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AgeCountryReducer extends Reducer<Text, IntWritable, Text, Text> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int minAge = Integer.MAX_VALUE;
        int maxAge = Integer.MIN_VALUE;
        int count = 0;

        for (IntWritable val : values) {
            int age = val.get();
            if (age < minAge) {
                minAge = age;
            }
            if (age > maxAge) {
                maxAge = age;
            }
            count++;
        }

        String result = String.format("Min Age: %d, Max Age: %d, Population: %d", minAge, maxAge, count);
        context.write(key, new Text(result));
    }
}

