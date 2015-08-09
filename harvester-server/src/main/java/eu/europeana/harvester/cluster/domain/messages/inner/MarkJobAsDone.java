package eu.europeana.harvester.cluster.domain.messages.inner;

import eu.europeana.harvester.cluster.domain.messages.DoneProcessing;

import java.io.Serializable;

public class MarkJobAsDone implements Serializable {

    private final DoneProcessing doneProcessing;

    public MarkJobAsDone(DoneProcessing doneProcessing) {
        this.doneProcessing = doneProcessing;
    }

    public DoneProcessing getDoneProcessing() {
        return doneProcessing;
    }
}
