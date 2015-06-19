package eu.europeana.uimtester.reportprocessingjobs.logic;

import eu.europeana.uimtester.reportprocessingjobs.domain.ProcessingJobWithStatsAndResults;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class ProcessingJobReport {
    final ProcessingJobReportRetriever processingJobReportRetriever;
    final ProcessingJobReportWriter processingJobReportWriter;

    public ProcessingJobReport(ProcessingJobReportRetriever processingJobReportRetriever, ProcessingJobReportWriter processingJobReportWriter) {
        this.processingJobReportRetriever = processingJobReportRetriever;
        this.processingJobReportWriter = processingJobReportWriter;
    }

    public void execute (final String inputFilePath) throws IOException {
        final List<String> jobIds = new LinkedList<>();

        final BufferedReader reader = Files.newBufferedReader(Paths.get(inputFilePath), Charset.defaultCharset());

        for (String line = reader.readLine(); null != line; line = reader.readLine()) {
            jobIds.add(line.trim());
        }

        System.out.println(jobIds.toString());

        final List<ProcessingJobWithStatsAndResults> stats = processingJobReportRetriever.generateReportOnProcessingJobs(jobIds);
        processingJobReportWriter.write(processingJobReportWriter.objectsToString(stats));
    }

}
