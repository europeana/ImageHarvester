package eu.europeana.uimtester.reportprocessingjobs.logic;

import eu.europeana.uimtester.reportprocessingjobs.domain.ProcessingJobWithStatsAndResults;

import java.util.List;

public class ProcessingJobReportWriter {

    public StringBuilder objectsToString(final List<ProcessingJobWithStatsAndResults> inputs){
        final StringBuilder output = new StringBuilder();
        for (final ProcessingJobWithStatsAndResults input : inputs) {
            output.append(objectToString(input)).append("\\n");
        }
        return output;
    }

    public StringBuilder objectToString(final ProcessingJobWithStatsAndResults input){
        final StringBuilder output = new StringBuilder();
        output.append("job.id").append(" = ").append(input.getProcessingJob().getId()).append("\\n");
        output.append("job.state").append(" = ").append(input.getProcessingJob().getState().name()).append("\\n");
        return output;
    }

    public void writeToConsole(final StringBuilder content) {
        System.out.println(content.toString());
    }
}
