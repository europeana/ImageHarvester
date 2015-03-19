package eu.europeana.harvester.utils;

import gr.ntua.image.mediachecker.MediaChecker;

import java.io.IOException;

/**
 * An utility class.
 */
public class LocalMediaChecker extends MediaChecker {

    public static Integer getdpi(String filename) throws Exception {
        return getDPI(filename );
    }

    public static Boolean issearchable(String filename) throws Exception {
        return isSearchable(filename);
    }
}
