package com.absorprofess.mapreduce2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMap extends Mapper<LongWritable, Text, Info, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] infos = line.split("\t");
        context.write(new Info(Long.parseLong(infos[infos.length-3]),Long.parseLong(infos[infos.length-2])),new Text(infos[0]));
    }
}
