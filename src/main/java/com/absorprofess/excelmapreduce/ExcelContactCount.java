package com.absorprofess.excelmapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ExcelContactCount extends Configured implements Tool {
    public static class PhoneMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
            Text pkey = new Text();
            Text pvalue = new Text();
            // 1.0, 老爸, 13999123786, 2014-12-20
            String line = value.toString();
            System.out.println(line);
           /* String[] records = line.split("\\s+");
            // 获取月份
            String[] months = records[3].split("-");
            // 昵称+月份
            pkey.set(records[1] + "\t" + months[1]);
            // 手机号
            pvalue.set(records[2]);*/

            context.write(pkey, pvalue);
        }
    }

    public static class PhoneReducer extends Reducer<Text, Text, Text, Text> {

        protected void reduce(Text Key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
            Text phone = Values.iterator().next();
            int phoneToal = 0;

            for (java.util.Iterator<Text> its = Values.iterator(); its.hasNext(); its.next()) {
                phoneToal++;
            }

            Text pvalue = new Text(phone + "\t" + phoneToal);

            context.write(Key, pvalue);
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public int run(String[] args) throws Exception {
        // 读取配置文件
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        // 判断输出路径，如果存在，则删除
        Path mypath = new Path(args[1]);
        FileSystem hdfs = mypath.getFileSystem(conf);
        if (hdfs.isDirectory(mypath)) {
            hdfs.delete(mypath, true);
        }

        // 新建任务
        Job job = new Job(conf, "Call Log");
        job.setJarByClass(ExcelContactCount.class);

        // 输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Mapper
        job.setMapperClass(PhoneMapper.class);
        // Reduce
        job.setReducerClass(PhoneReducer.class);

        // 输出key类型
        job.setOutputKeyClass(Text.class);
        // 输出value类型
        job.setOutputValueClass(Text.class);

        // 自定义输入格式
        job.setInputFormatClass(ExcelInputFormat.class);
        // 自定义输出格式
        job.setOutputFormatClass(ExcelOutputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        String[] args0 = {
                "hdfs://node01:8020/huohua/ods_rawdata/2018-11-13/d03620a5-808c-4c58-b91e-5853911de936.xls",
                "hdfs://node01:8020/out"
        };
        int ec = ToolRunner.run(new Configuration(), new ExcelContactCount(), args0);
        System.exit(ec);
    }
}
