package eu.europeana.harvester.cluster.domain;

public enum TaskState {
    READY,
    DOWNLOADING,
    PROCESSING,
    DONE,
    PAUSE
}
