package eu.europeana.uimtester.reportprocessingjobs.logic;

import eu.europeana.uimtester.reportprocessingjobs.domain.ProcessingJobWithStatsAndResults;

import java.io.BufferedReader;
import java.io.File;
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

    public void execute (final File inputFilePath) throws IOException {
        if (null == inputFilePath) {
            throw new IllegalArgumentException("inputFile is null");
        }
        final List<String> jobIds = new LinkedList<>();

        final BufferedReader reader = Files.newBufferedReader(inputFilePath.toPath(), Charset.defaultCharset());

        for (String line = reader.readLine(); null != line; line = reader.readLine()) {
            jobIds.add(line.trim());
        }

        final List<ProcessingJobWithStatsAndResults> stats = processingJobReportRetriever.generateReportOnProcessingJobs(jobIds);
        processingJobReportWriter.write(processingJobReportWriter.objectsToString(stats));
    }

}
