package com.opstty.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class MaxiHeightMapperTest {
    @Mock
    private Mapper.Context context;
    private MaxiHeightMapper maxiheightMapper;

    @Before
    public void setup(){
        this.maxiheightMapper =new MaxiHeightMapper();
    }

    @Test
    public void TestMap() throws IOException, InterruptedException{
        String t1 = "((48.8669690843, 2.31951408752);8;Sequoiadendron;giganteum;Taxodiaceae;1850;20.0;320.0;Cours-la-Reine, avenue Franklin-D.-Roosevelt, avenue Matignon, avenue Gabriel;Séquoia géant;;12;Jardin des Champs Elysées)";
        this.maxiheightMapper.map(null, new Text(t1), this.context);
        verify(this.context, times(1))
                .write(new Text("giganteum"), new IntWritable(20));
    }
}
