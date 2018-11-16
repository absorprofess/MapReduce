package cn.eone.excelmapreduce;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class ExcelOutputFormat extends FileOutputFormat<Text, Text> {
    // MultiRecordWriter对象
    private MultiRecordWriter writer = null;

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext job) throws IOException,
            InterruptedException {
        if (writer == null) {
            writer = new MultiRecordWriter(job, getTaskOutputPath(job));
        }

        return writer;
    }

    private Path getTaskOutputPath(TaskAttemptContext conf) throws IOException {
        Path workPath = null;

        OutputCommitter committer = super.getOutputCommitter(conf);

        if (committer instanceof FileOutputCommitter) {
            workPath = ((FileOutputCommitter) committer).getWorkPath();
        } else {
            Path outputPath = super.getOutputPath(conf);
            if (outputPath == null) {
                throw new IOException("没有定义输出目录");
            }
            workPath = outputPath;
        }

        return workPath;
    }

    /**
     * 通过key, value, conf来确定输出文件名（含扩展名）
     *
     * @param key
     * @param value
     * @param conf
     * @return String
     */
    protected String generateFileNameForKeyValue(Text key, Text value, Configuration conf) {
        // name + month
        String[] records = key.toString().split(".");
        return "test.txt";
    }

    /**
     * 定义MultiRecordWriter
     */
    public class MultiRecordWriter extends RecordWriter<Text, Text> {
        // RecordWriter的缓存
        private HashMap<String, RecordWriter<Text, Text>> recordWriters = null;
        // TaskAttemptContext
        private TaskAttemptContext job = null;
        // 输出目录
        private Path workPath = null;

        public MultiRecordWriter(TaskAttemptContext job, Path workPath) {
            super();
            this.job = job;
            this.workPath = workPath;
            this.recordWriters = new HashMap<String, RecordWriter<Text, Text>>();
        }

        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {
            // 得到输出文件名
            String baseName = generateFileNameForKeyValue(key, value, job.getConfiguration());
            RecordWriter<Text, Text> rw = this.recordWriters.get(baseName);
            if (rw == null) {
                rw = getBaseRecordWriter(job, baseName);
                this.recordWriters.put(baseName, rw);
            }
            rw.write(key, value);
        }

        private RecordWriter<Text, Text> getBaseRecordWriter(TaskAttemptContext job, String baseName)
                throws IOException, InterruptedException {
            Configuration conf = job.getConfiguration();

            boolean isCompressed = getCompressOutput(job);
            //key value 分隔符
            String keyValueSeparator = "\t";

            RecordWriter<Text, Text> recordWriter = null;
            if (isCompressed) {
                Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job,
                        GzipCodec.class);
                CompressionCodec codec = ReflectionUtils.newInstance(codecClass, conf);

                Path file = new Path(workPath, baseName + codec.getDefaultExtension());

                FSDataOutputStream fileOut = file.getFileSystem(conf).create(file, false);

                recordWriter = new MailRecordWriter<Text, Text>(new DataOutputStream(codec
                        .createOutputStream(fileOut)), keyValueSeparator);
            } else {
                Path file = new Path(workPath, baseName);

                FSDataOutputStream fileOut = file.getFileSystem(conf).create(file, false);

                recordWriter = new MailRecordWriter<Text, Text>(fileOut, keyValueSeparator);
            }

            return recordWriter;
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            Iterator<RecordWriter<Text, Text>> values = this.recordWriters.values().iterator();
            while (values.hasNext()) {
                values.next().close(context);
            }
            this.recordWriters.clear();
        }

    }
}
