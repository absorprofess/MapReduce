package cn.eone.TOPN;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class TopN {
    static class TopNMap extends Mapper<LongWritable, Text, NullWritable, IntWritable> {
        private static IntWritable valueInfo = new IntWritable();
        TreeSet<Integer> topNums = new TreeSet<Integer>(new
                                                            Comparator<Integer>() {
                                                                /*
                                                                 * int compare(Object o1, Object o2) 返回一个基本类型的整型，
                                                                 * 返回负数表示：o1 小于 o2，
                                                                 * 返回 0 表示：o1 和 o2 相等，
                                                                 * 返回正数表示：o1 大于 o2。
                                                                 * 谁大谁排后面
                                                                 */
                                                                public int compare(Integer a, Integer b) {
                                                                    return b - a;
                                                                }
                                                            });
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] nums = value.toString().split(" ");
            for (String num : nums) {
                topNums.add(Integer.parseInt(num));
                if(topNums.size()>5){
                    topNums.remove(topNums.size());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer topNum : topNums) {
                valueInfo.set(topNum);
                context.write(NullWritable.get(), valueInfo);
            }

        }
    }

    static class TopNReduce extends Reducer<NullWritable, IntWritable, NullWritable, IntWritable> {
        TreeSet<Integer> topNums = new TreeSet<Integer>(new
                                                         Comparator<Integer>() {
                                                             /*
                                                              * int compare(Object o1, Object o2) 返回一个基本类型的整型，
                                                              * 返回负数表示：o1 小于 o2，
                                                              * 返回 0 表示：o1 和 o2 相等，
                                                              * 返回正数表示：o1 大于 o2。
                                                              * 谁大谁排后面
                                                              */
                                                             public int compare(Integer a, Integer b) {
                                                                 return b - a;
                                                             }
                                                         });
        private static IntWritable valueInfo = new IntWritable();
        @Override
        protected void reduce(NullWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable num : values) {
                topNums.add(num.get());
                if(topNums.size()>5){
                    topNums.remove(topNums.size());
                }
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Integer topNum : topNums) {
                valueInfo.set(topNum);
                context.write(NullWritable.get(), valueInfo);
            }

        }
    }


    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.4");
        Configuration conf = new Configuration();
        Job wcjob = Job.getInstance(conf);
//指定我这个 job 所在的 jar 包
// wcjob.setJar("/home/hadoop/wordcount.jar");
        wcjob.setJarByClass(TopN.class);
        wcjob.setMapperClass(TopNMap.class);
        wcjob.setReducerClass(TopNReduce.class);
//设置我们的业务逻辑 Mapper 类的输出 key 和 value 的数据类型
        wcjob.setMapOutputKeyClass(NullWritable.class);
        wcjob.setMapOutputValueClass(IntWritable.class);
//设置我们的业务逻辑 Reducer 类的输出 key 和 value 的数据类型
        wcjob.setOutputKeyClass(NullWritable.class);
        wcjob.setOutputValueClass(IntWritable.class);
        //wcjob.setNumReduceTasks(6);
        //wcjob.setPartitionerClass(ProvincePartitioner.class);
//指定要处理的数据所在的位置
        FileInputFormat.setInputPaths(wcjob, "D:\\TopN");
//指定处理完成之后的结果所保存的位置
        FileOutputFormat.setOutputPath(wcjob, new Path("D:\\TopN\\output"));
//向 yarn 集群提交这个 job
        boolean res = wcjob.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

}
