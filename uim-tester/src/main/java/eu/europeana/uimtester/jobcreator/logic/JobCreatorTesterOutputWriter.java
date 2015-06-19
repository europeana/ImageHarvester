package eu.europeana.uimtester.jobcreator.logic;

import com.google.code.morphia.Key;
import eu.europeana.harvester.domain.ProcessingJob;

import java.io.IOException;
import java.io.Writer;

public class JobCreatorTesterOutputWriter {
    private final Writer writer;

    public JobCreatorTesterOutputWriter (Writer writer) {this.writer = writer;}


    public void write (Iterable<Key<ProcessingJob>> processingJobs) throws IOException {
        if (null == processingJobs) {
            return ;
        }

        for (final Key<ProcessingJob> processingJob: processingJobs) {
            writer.write(processingJob.getId().toString());
            writer.write('\n');
        }

        writer.flush();
    }

    public Writer getWriter () {
        return writer;
    }
}
