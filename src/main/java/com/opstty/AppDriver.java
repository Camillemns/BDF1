package com.opstty;

import com.opstty.job.DistrictTrees;
import com.opstty.job.NumberSpecies;
import com.opstty.job.Species;
import com.opstty.job.WordCount;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");
            programDriver.addClass("districtTrees", DistrictTrees.class, "A map/reduce program that counts the number of district with trees");
            programDriver.addClass("species", Species.class, "A map/reduce program that display the list of species");
            programDriver.addClass("numberSpe", NumberSpecies.class, "A map/reduce program that display the number of species");
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
