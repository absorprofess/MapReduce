package com.absorprofess.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Info implements WritableComparable<Info> {

    private Long id;
    private double oxy;
    private double ph;
    private double flow;
    private String date;


    public Info() {
    }

    public Info(Long id, double oxy, double ph, double flow, String date) {
        this.id = id;
        this.oxy = oxy;
        this.ph = ph;
        this.flow = flow;
        this.date = date;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public double getOxy() {
        return oxy;
    }

    public void setOxy(double oxy) {
        this.oxy = oxy;
    }

    public double getPh() {
        return ph;
    }

    public void setPh(double ph) {
        this.ph = ph;
    }

    public double getFlow() {
        return flow;
    }

    public void setFlow(double flow) {
        this.flow = flow;
    }


    @Override
    public int compareTo(Info o) {
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        dataOutput.writeDouble(oxy);
        dataOutput.writeDouble(ph);
        dataOutput.writeDouble(flow);
        dataOutput.writeUTF(date);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readLong();
        this.oxy = dataInput.readDouble();
        this.ph = dataInput.readDouble();
        this.flow = dataInput.readDouble();
        this.date = dataInput.readUTF();
    }

    @Override
    public String toString() {
        return id + "\001" + oxy + "\001" + ph + "\001" + flow + "\001" + date;
    }
}
