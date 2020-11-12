package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import java.io.IOException;
import java.util.Arrays;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class NumberSpeciesReducerTest {

    @Mock
    private Reducer.Context context;
    private NumberSpeciesReducer countspeciesReducer;

    @Before
    public void setup(){
        this.countspeciesReducer =new NumberSpeciesReducer();
    }

    @Test
    public void TestMap() throws IOException, InterruptedException{
        String key = "giganteum";
        IntWritable value = new IntWritable(1);
        Iterable<IntWritable> values = Arrays.asList(value,value,value,value,value);
        this.countspeciesReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key),  new IntWritable(5));
    }
}
