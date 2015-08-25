package eu.europeana.harvester.utils;

import akka.event.LoggingAdapter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by paul on 04/03/15.
 */
public class FileUtils {


    /**
     * Creates a file for metadata extraction and thumbnail generation.
     */
    public final static String createFileAndFolderIfMissing(final LoggingAdapter LOG,final File folder,final String fileName,final byte[] fileContent) {
        //final File folder = new File("/tmp/europeana");
        if (!folder.exists()) {
            folder.mkdir();
        }

        final String temporaryPath = folder.getAbsolutePath() + fileName;

        FileOutputStream fileOutputStream = null;
        try {
            fileOutputStream = new FileOutputStream(temporaryPath);
            fileOutputStream.write(fileContent);
            fileOutputStream.close();
        } catch (IOException e) {
            LOG.error("In ProcesserSlaveActor: error at creating a file");
        } finally {
            if (fileOutputStream != null) {
                try {
                    fileOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return temporaryPath;
    }

}
