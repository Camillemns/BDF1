package com.opstty.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
public class DistrictTreeReducerTest {

    @Mock
    private Reducer.Context context;
    private DistrictTreeReducer districtReducer;

    @Before
    public void setup(){
        this.districtReducer =new DistrictTreeReducer();
    }

    @Test
    public void TestMap() throws IOException, InterruptedException{
        String key = "7";
        IntWritable value = new IntWritable(1);
        Iterable<NullWritable> values = Arrays.asList();
        this.districtReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key),  NullWritable.get());
    }
}
