package eu.europeana.harvester.domain;


/**
 * Created by salexandru on 28.07.2015.
 */
public enum ProcessingStatus {
    //all subtasks have finished and have the ProcessingJobSubTaskState.SUCCESS
    Success,

    //at least one subtask has finished with an error
    Failure,

    //if not all sub tasks have finished processing
    Unknown,
}
