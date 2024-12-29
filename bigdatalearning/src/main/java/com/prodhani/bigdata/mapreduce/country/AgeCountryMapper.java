package com.prodhani.bigdata.mapreduce.country;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AgeCountryMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text country = new Text();
    private IntWritable age = new IntWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header line
        if (value.toString().startsWith("ID")) {
            return;
        }

        String[] fields = value.toString().split(",");
        if (fields.length == 4) {
            try {
                int ageValue = Integer.parseInt(fields[2].trim());
                String countryValue = fields[3].trim();
                country.set(countryValue);
                age.set(ageValue);
                context.write(country, age);
            } catch (NumberFormatException e) {
                // Handle invalid age value
            }
        }
    }
}
