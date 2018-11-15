package com.absorprofess.mapreduce2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner  extends Partitioner<Info,Text> {

    @Override
    public int getPartition(Info info, Text text, int numPartitions) {
        String substring = text.toString().substring(0, 3);
        switch (substring){
            case  "134":{
                return 0;
            }
            case "135" :{
                return 1;
            }
            case "136" :{
                return 2;
            }
            case "137" :{
                return 3;
            }
            case "138" :{
                return 4;
            }
            default:{
                return 5;
            }
        }

    }
}
