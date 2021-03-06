package cn.eone.excelmapreduce.creativereport;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ExcelContactCount extends Configured implements Tool {
    public static class HandleMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        public void map(LongWritable key, Text value, Context context) throws InterruptedException, IOException {
            context.write(new Text(value.toString()), NullWritable.get());
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public int run(String[] args) throws Exception {
        // 读取配置文件
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node01:8020");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        //conf.set("dfs.client.use.datanode.hostname","true");
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
        job.setMapperClass(HandleMapper.class);
        job.setNumReduceTasks(1);

        // 输出key类型
        job.setMapOutputKeyClass(Text.class);
        // 输出value类型
        job.setMapOutputValueClass(NullWritable.class);

        // 自定义输入格式
        job.setInputFormatClass(ExcelInputFormat.class);


        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int ec = ToolRunner.run(new Configuration(), new ExcelContactCount(), args);
        System.exit(ec);
    }
}
