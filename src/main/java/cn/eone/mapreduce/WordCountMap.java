package cn.eone.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMap extends Mapper<LongWritable, Text, NullWritable, Info> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        System.out.println(line);
        String[] infos = line.split(" ");
        context.write(NullWritable.get(),new Info(Long.parseLong(infos[2]),Double.parseDouble(infos[3]),Double.parseDouble(infos[4]),Double.parseDouble(infos[5]),infos[1]));
    }
}
