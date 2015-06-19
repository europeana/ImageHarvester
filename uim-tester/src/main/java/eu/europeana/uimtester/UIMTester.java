package eu.europeana.uimtester;

import eu.europeana.harvester.client.HarvesterClient;
import eu.europeana.uimtester.jobcreator.logic.JobCreatorTester;
import eu.europeana.uimtester.reportprocessingjobs.logic.ProcessingJobReport;

/**
 * Created by paul on 19/06/15.
 */
public class UIMTester {

    // TODO : Initialise
    private static HarvesterClient harvesterClient = null;
    private static JobCreatorTester jobCreatorTester = new JobCreatorTester(null);
    private static ProcessingJobReport report = new ProcessingJobReport(null,null);

    // TODO : Implement
    public static void main(String args[]) {

        // Use case 1 : program job-creator uim-tester.conf job-creator-input.conf job-creator-output.conf
        // Use case 2 : program report-processing uim-tester.conf job-creator-output.conf report-processing-out.conf

    }
}
