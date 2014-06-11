package test;

import java.io.*;

public class LinkParser {

    private final int nrOfLinks;
    private final String outputFileName;

    public LinkParser(int nrOfLinks, String outputFileName) {
        this.nrOfLinks = nrOfLinks;
        this.outputFileName = outputFileName;
    }

    public void start()  {
        final int nrOfFiles = nrOfFiles();
        final int linesPerFile = nrOfLinks / nrOfFiles;
        final int oneLinkPerFile = nrOfLinks - linesPerFile * nrOfFiles;

        File folder = new File("./src/main/resources/SampleLinks");
        File output = new File("./src/main/resources/TestLinks/" + outputFileName);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(output));
        } catch (IOException e) {
            e.printStackTrace();
        }
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
            int j = 0;
            int nr = (i<oneLinkPerFile) ? linesPerFile+1 : linesPerFile;
            if (listOfFiles[i].isFile()) {
                BufferedReader br = null;
                try {
                    br = new BufferedReader(new FileReader(listOfFiles[i]));
                    String line;
                    try {
                        while ((line = br.readLine()) != null && j < nr) {
                            j++;
                            if(line.length() != 0) {
                                if(line.charAt(line.length()-1) != '\n' && line.toLowerCase().contains("http")) {
                                    bw.write(line.substring(line.toLowerCase().indexOf("http")).trim().toLowerCase() + "\n");
                                } else {
                                    bw.write(line);
                                }
                            }
                        }
                        br.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            bw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private int nrOfFiles() {
        File folder = new File("./src/main/resources/SampleLinks");
        File[] listOfFiles = folder.listFiles();

        return listOfFiles.length;
    }

}
