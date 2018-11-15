package com.absorprofess.mapreduce2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Info implements WritableComparable<Info> {
    Long up;
    Long down;
    Long sum;

    public Info(Long up, Long down) {
        this.up = up;
        this.down = down;
        this.sum = up+down;
    }

    public Info() {
    }

    public Info(Long up, Long down, Long sum) {
        this.up = up;
        this.down = down;
        this.sum = sum;
    }

    public Long getUp() {
        return up;
    }

    public void setUp(Long up) {
        this.up = up;
    }

    public Long getDown() {
        return down;
    }

    public void setDown(Long down) {
        this.down = down;
    }

    public Long getSum() {
        return sum;
    }

    public void setSum(Long sum) {
        this.sum = sum;
    }

    @Override
    public String toString() {
        return up +
                "\t" + down +
                "\t" + sum;
    }

    @Override
    public int compareTo(Info o) {
        return this.sum<o.sum?1:-1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(up);
        out.writeLong(down);
        out.writeLong(sum);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.up = in.readLong();
        this.down = in.readLong();
        this.sum = in.readLong();
    }
}
