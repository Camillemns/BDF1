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
public class NumberSpeciesMapperTest {

    @Mock
    private Mapper.Context context;
    private NumberSpeciesMapper countspeciesMapper;

    @Before
    public void setup(){
        this.countspeciesMapper =new NumberSpeciesMapper();
    }

    @Test
    public void TestMap() throws IOException, InterruptedException{
        String t1 = "((48.8669690843, 2.31951408752);8;Sequoiadendron;giganteum;Taxodiaceae;1850;20.0;320.0;Cours-la-Reine, avenue Franklin-D.-Roosevelt, avenue Matignon, avenue Gabriel;Séquoia géant;;12;Jardin des Champs Elysées)";
        String t2 = "((48.879759998, 2.38064802989);19;Sequoiadendron;giganteum;Taxodiaceae;;35.0;470.0;Rue Manin, rue Botzaris;Séquoia géant;;57;Parc des Buttes Chaumont)";
        String t3 = "((48.8693939056, 2.24776773334);16;Sequoiadendron;giganteum;Taxodiaceae;1850;30.0;490.0;Allée de Longchamp, route de Sèvres à Neuilly;Séquoia géant;;72;Bois de Boulogne (Bagatelle))";

        this.countspeciesMapper.map(null, new Text(t1), this.context);
        this.countspeciesMapper.map(null, new Text(t2), this.context);
        this.countspeciesMapper.map(null, new Text(t3), this.context);
        verify(this.context, times(3))
                .write(new Text("giganteum"), new IntWritable(1));

    }
}
