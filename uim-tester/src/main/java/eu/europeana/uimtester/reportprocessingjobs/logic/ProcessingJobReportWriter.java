package eu.europeana.uimtester.reportprocessingjobs.logic;

import eu.europeana.harvester.domain.SourceDocumentProcessingStatistics;
import eu.europeana.uimtester.reportprocessingjobs.domain.ProcessingJobWithStatsAndResults;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.Map;

public class ProcessingJobReportWriter {
    private  final Writer writer;
    private final boolean printMore;

    public ProcessingJobReportWriter (final Writer writer, boolean printMore) {
       this.writer = writer;
        this.printMore = printMore;
    }

    public String objectsToString(final List<ProcessingJobWithStatsAndResults> inputs){
        final StringBuilder output = new StringBuilder();

        output.append("processingJobStats = [\n");
        for (final ProcessingJobWithStatsAndResults input : inputs) {
            output.append("\t").append(objectToString(input)).append("\n");
        }
        output.append("]\n");
        return output.toString();
    }

    public String objectToString(final ProcessingJobWithStatsAndResults input){
        final StringBuilder output = new StringBuilder();
        output.append("\t {");
        output.append("\t\t").append("job.id").append(" = ").append(input.getProcessingJob().getId()).append("\n");
        output.append("\t\t").append("job.state").append(" = ").append(input.getProcessingJob().getState().name()).append("\n");

        if (printMore) {
            output.append("\t\t").append("taskStatistics = [\n");
            for (final Map.Entry<String, SourceDocumentProcessingStatistics> entry: input.getSourceDocumentReferenceIdToStatsMap().entrySet()) {
                final SourceDocumentProcessingStatistics stats = entry.getValue();

                output.append("\t\t\t {\n");
                output.append("\t\t\t\t").append("taskType = " + stats.getTaskType()).append("\n");
                output.append("\t\t\t\t").append("createdAt = " + stats.getCreatedAt()).append("\n");
                output.append("\t\t\t\t").append("state = " + stats.getState()).append("\n");
                output.append("\t\t\t\t").append("isActive = " + stats.getActive()).append("\n");
                output.append("\t\t\t\t").append("httpCode = " + stats.getHttpResponseCode()).append("\n");
                output.append("\t\t\t\t").append("httpContentType = " + stats.getHttpResponseContentType()).append("\n");
                output.append("\t\t\t\t").append("httpContentSize = " + stats.getHttpResponseContentSizeInBytes()).append("B\n");
                output.append("\t\t\t\t").append("log = " + stats.getLog()).append("\n");
                output.append("\t\t\t }\n");
            }
            output.append("\t\t]\n");
        }

        output.append("\t }\n");
        return output.toString();
    }

    public void write(final String content) throws IOException {
        writer.write(content);
        writer.flush();
    }

    public Writer getWriter () {
        return writer;
    }

    public boolean isPrintMore () {
        return printMore;
    }
}
