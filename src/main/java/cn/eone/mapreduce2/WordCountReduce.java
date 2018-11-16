package cn.eone.mapreduce2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class  WordCountReduce extends Reducer <Info,Text,Text,Info>{
    @Override
    protected void reduce(Info key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(values.iterator().next(),key);
    }
}
