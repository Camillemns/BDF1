package com.opstty;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Informations implements Writable {
    private Text district;
    private IntWritable age;

    public Informations() {
        district = new Text();
        age = new IntWritable();
    }

    public Informations(Text district,IntWritable age) {
        this.district = district;
        this.age = age;
    }

    public Text getDistrict() {
        return district;
    }

    public IntWritable getAge() {
        return age;
    }

    public void setAge(IntWritable age) {
        this.age = age;
    }

    public void setDistrict(Text district) {
        this.district = district;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        age.write(dataOutput);
        district.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        age.readFields(dataInput);
        district.readFields(dataInput);
    }
}
