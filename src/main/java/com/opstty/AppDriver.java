package com.opstty;

import com.opstty.job.*;
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
            programDriver.addClass("maxiHeight", MaxiHeight.class, "A map/reduce program that find the maximum height of each tree by species");
            programDriver.addClass("sortHeight", SortHeight.class, "A map/reduce program that sort height of each tree");
            programDriver.addClass("oldestTree", OldestTree.class, "A map/reduce program that display the district with the oldest tree");
            programDriver.addClass("mostTree", MostTree.class, "A map/reduce program that display the district with the more trees");
            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
