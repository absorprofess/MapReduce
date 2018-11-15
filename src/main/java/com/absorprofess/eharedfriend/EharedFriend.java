package com.absorprofess.eharedfriend;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Comparator;
import java.util.TreeSet;

public class EharedFriend {
    static class EharedFriendMap extends Mapper<LongWritable, Text, Text, Text> {
        private static Text keyInfo = new Text();
        private static Text valueInfo = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] name = value.toString().split(":");
            String[] friends = name[1].split(",");
           /* for (int i=0;i<friends.length-1;i++) {
               for(int j=i+1;j<friends.length;j++){
                   keyInfo.set(friends[i]+"-"+friends[j]);

                   valueInfo.set(name[0]);
                   System.out.println(keyInfo.toString()+":"+valueInfo.toString());
                   context.write(keyInfo,valueInfo);
             }
            }*/
            for (String friend : friends) {
                keyInfo.set(friend);
                valueInfo.set(name[0]);
                context.write(keyInfo,valueInfo);
            }
        }
    }

    static class EharedFriendReduce extends Reducer<Text, Text, Text, Text> {
        private static Text valueInfo = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String friend="";
            for (Text num : values) {

                friend+=num.toString()+",";
            }
            valueInfo.set(friend);
            context.write(key,valueInfo);
        }
    }


    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4");
        Configuration conf = new Configuration();
        Job wcjob = Job.getInstance(conf);
//指定我这个 job 所在的 jar 包
// wcjob.setJar("/home/hadoop/wordcount.jar");
        wcjob.setJarByClass(EharedFriend.class);
        wcjob.setMapperClass(EharedFriendMap.class);
        wcjob.setReducerClass(EharedFriendReduce.class);
//设置我们的业务逻辑 Mapper 类的输出 key 和 value 的数据类型
        wcjob.setMapOutputKeyClass(Text.class);
        wcjob.setMapOutputValueClass(Text.class);
//设置我们的业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        wcjob.setOutputKeyClass(Text.class);
        wcjob.setOutputValueClass(Text.class);
        //wcjob.setNumReduceTasks(6);
        //wcjob.setPartitionerClass(ProvincePartitioner.class);
//指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(wcjob, "D:\\EharedFriend");
//指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(wcjob, new Path("D:\\EharedFriend\\output"));
//向 yarn 集群提交这个 job
        boolean res = wcjob.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

}
