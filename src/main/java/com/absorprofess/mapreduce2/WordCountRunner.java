package com.absorprofess.mapreduce2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountRunner {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job wcjob = Job.getInstance(conf);
//指定我这个 job 所在的 jar 包
// wcjob.setJar("/home/hadoop/wordcount.jar");
        wcjob.setJarByClass(WordCountRunner.class);
        wcjob.setMapperClass(WordCountMap.class);
        wcjob.setReducerClass(WordCountReduce.class);
//设置我们的业务逻辑 Mapper 类的输出 key 和 value 的数据类型
        wcjob.setMapOutputKeyClass(Info.class);
        wcjob.setMapOutputValueClass(Text.class);
//设置我们的业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(Info.class);
        wcjob.setNumReduceTasks(6);
        wcjob.setPartitionerClass(ProvincePartitioner.class);
//指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(wcjob, "D:\\data\\output");
//指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(wcjob, new Path("D:\\data\\output1"));
//向 yarn 集群提交这个 job
        boolean res = wcjob.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

}
