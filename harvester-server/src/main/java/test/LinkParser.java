package test;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;

public class LinkParser {

    private static final Logger LOG = LogManager.getLogger(LinkParser.class.getName());

    private final int nrOfLinks;
    private final String outputFileName;

    public LinkParser(int nrOfLinks, String outputFileName) {
        this.nrOfLinks = nrOfLinks;
        this.outputFileName = outputFileName;
    }

    public void start()  {
        final File folder = new File("./harvester-client/src/main/resources/SampleLinks");
        final File output = new File("./harvester-client/src/main/resources/TestLinks/" + outputFileName);

        if(!output.exists()) {
            try {
                output.createNewFile();
            } catch (IOException e) {
                LOG.error(e.getMessage());
            }
        }

        final int nrOfFiles = nrOfFiles(folder);
        int linesPerFile = nrOfLinks / nrOfFiles;
        int oneLinkPerFile = nrOfLinks - linesPerFile * nrOfFiles;

        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(output));
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        final File[] listOfFiles = folder.listFiles();
        for (int i = 0; i < listOfFiles.length; i++) {
            int j = 0;
            int nr = (i<oneLinkPerFile) ? linesPerFile+1 : linesPerFile;
            if (listOfFiles[i].isFile()) {
                try {
                    final BufferedReader br = new BufferedReader(new FileReader(listOfFiles[i]));
                    String line;
                    try {
                        while ((line = br.readLine()) != null && j < nr) {
                            if(line.length() != 0 && line.toLowerCase().contains("http")) {
                                j++;
                                if(line.charAt(line.length()-1) != '\n') {
                                    bw.write(line.substring(line.toLowerCase().indexOf("http")).trim().toLowerCase() + "\n");
                                } else {
                                    bw.write(line);
                                }
                            }
                        }
                        if(j < nr) {
                            linesPerFile += (nr - j) / (listOfFiles.length - i);
                        }
                        br.close();
                    } catch (IOException e) {
                        LOG.error(e.getMessage());
                    }
                } catch (FileNotFoundException e) {
                    LOG.error(e.getMessage());
                }
            }
        }
        try {
            bw.close();
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    private int nrOfFiles(File folder) {
        File[] listOfFiles = folder.listFiles();

        return listOfFiles.length;
    }

}
