package com.opstty.mapper;

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
import org.apache.hadoop.io.NullWritable;

@RunWith(MockitoJUnitRunner.class)
public class DistrictTreeMapperTest {

    @Mock
    private Mapper.Context context;
    private DistrictTreeMapper districtMapper;


    @Before
    public void setup(){
        this.districtMapper =new DistrictTreeMapper();
    }

    @Test
    public void TestMap() throws IOException, InterruptedException{
        String mytxt = "(48.857140829, 2.29533455314);7;Maclura;pomifera;Moraceae;1935;13.0;;Quai Branly, avenue de La Motte-Piquet, avenue de la Bourdonnais, avenue de Suffren;Oranger des Osages;;6;Parc du Champs de Mars)";
        this.districtMapper.map(null, new Text(mytxt), this.context);
        verify(this.context, times(1))
                .write(new Text("7"), NullWritable.get());
    }
}

