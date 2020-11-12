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
public class MaxiHeightReducerTest {

    @Mock
    private Reducer.Context context;
    private MaxiHeightReducer maxiheightReducer;

    @Before
    public void setup(){
        this.maxiheightReducer =new MaxiHeightReducer();
    }

    @Test
    public void TestMap() throws IOException, InterruptedException{
        String key = "giganteum";
        IntWritable value = new IntWritable(35);
        IntWritable value1 = new IntWritable(30);
        IntWritable value2 = new IntWritable(20);

        Iterable<IntWritable> values = Arrays.asList(value1,value2,value2,value1,value);
        this.maxiheightReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key),  new IntWritable(35));
    }
}
