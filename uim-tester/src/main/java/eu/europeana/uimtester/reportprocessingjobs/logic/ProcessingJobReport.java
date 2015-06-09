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

    public void execute(final List<String> jobIds) {
        final List<ProcessingJobWithStatsAndResults> stats = processingJobReportRetriever.generateReportOnProcessingJobs(jobIds);
        processingJobReportWriter.writeToConsole(processingJobReportWriter.objectsToString(stats));
    }
}
