package eu.europeana.uimtester.reportprocessingjobs.logic;

import eu.europeana.uimtester.reportprocessingjobs.domain.ProcessingJobWithStatsAndResults;

import java.util.List;

public class ProcessingJobReport {
    final ProcessingJobReportRetriever processingJobReportRetriever;
    final ProcessingJobReportWriter processingJobReportWriter;

    public ProcessingJobReport(ProcessingJobReportRetriever processingJobReportRetriever, ProcessingJobReportWriter processingJobReportWriter) {
        this.processingJobReportRetriever = processingJobReportRetriever;
        this.processingJobReportWriter = processingJobReportWriter;
    }

    // TODO : Implement
    public void execute(final String inputFilePath,final String outputFilePath) {
        final List<String> jobIds = null; // (1) read job ids from input file
        final List<ProcessingJobWithStatsAndResults> stats = processingJobReportRetriever.generateReportOnProcessingJobs(jobIds);
        processingJobReportWriter.writeToConsole(processingJobReportWriter.objectsToString(stats));
    }

}
