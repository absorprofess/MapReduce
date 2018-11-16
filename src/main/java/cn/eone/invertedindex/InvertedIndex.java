package cn.eone.invertedindex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InvertedIndex {
    static class InvertedIndexMap extends Mapper<LongWritable, Text, Text, Text> {
        private static Text keyInfo = new Text();
        private static Text valueInfo = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            valueInfo.set(fileName);
            for (String word : words) {
                keyInfo.set(word);
                context.write(keyInfo, valueInfo);
            }

        }
    }

    static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {

        private static Text valueInfo = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String filename="";
            int count = 0;
            for (Text value : values) {
                count ++;
                filename=value.toString();
            }
            valueInfo.set(filename+":"+count+",");
            context.write(key, valueInfo);
        }
    }

    static class InvertedIndexReduce extends Reducer<Text, Text, Text, Text> {
        String string ="";
        private static Text valueInfo = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                string += value.toString();
            }
            valueInfo.set(string);
            context.write(key, valueInfo);
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4");
        Configuration conf = new Configuration();
        Job wcjob = Job.getInstance(conf);
//指定我这个 job 所在的 jar 包
// wcjob.setJar("/home/hadoop/wordcount.jar");
        wcjob.setJarByClass(InvertedIndex.class);
        wcjob.setMapperClass(InvertedIndexMap.class);
        wcjob.setCombinerClass(InvertedIndexCombiner.class);
        wcjob.setReducerClass(InvertedIndexReduce.class);
//设置我们的业务逻辑 Mapper 类的输出 key 和 value 的数据类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(Text.class);
//设置我们的业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(Text.class);
        //wcjob.setNumReduceTasks(6);
        //wcjob.setPartitionerClass(ProvincePartitioner.class);
//指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(wcjob, "D:\\InvertedIndex");
//指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(wcjob, new Path("D:\\InvertedIndex\\output"));
//向 yarn 集群提交这个 job
        boolean res = wcjob.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}
