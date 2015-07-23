package eu.europeana.uimtester.jobcreator.logic;

import eu.europeana.jobcreator.domain.ProcessingJobTuple;

import java.io.IOException;
import java.io.Writer;

public class JobCreatorTesterOutputWriter {
    private final Writer writer;

    public JobCreatorTesterOutputWriter (Writer writer) {this.writer = writer;}


    public void write (Iterable<ProcessingJobTuple> processingJobs) throws IOException {
        if (null == processingJobs) {
            return ;
        }

        for (final ProcessingJobTuple processingJob: processingJobs) {
            writer.write(processingJob.getProcessingJob().getId());
            writer.write('\n');
        }

        writer.flush();
    }

    public Writer getWriter () {
        return writer;
    }
}
