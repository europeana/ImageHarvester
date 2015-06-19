package eu.europeana.uimtester;

import com.google.code.morphia.Datastore;
import com.google.code.morphia.Morphia;
import com.mongodb.Mongo;
import eu.europeana.harvester.client.HarvesterClient;
import eu.europeana.harvester.client.HarvesterClientConfig;
import eu.europeana.harvester.client.HarvesterClientImpl;
import eu.europeana.harvester.db.mongo.*;
import eu.europeana.uimtester.domain.UIMTesterConfig;
import eu.europeana.uimtester.jobcreator.logic.JobCreatorTester;
import eu.europeana.uimtester.jobcreator.logic.JobCreatorTesterOutputWriter;
import eu.europeana.uimtester.reportprocessingjobs.logic.ProcessingJobReport;
import eu.europeana.uimtester.reportprocessingjobs.logic.ProcessingJobReportRetriever;
import eu.europeana.uimtester.reportprocessingjobs.logic.ProcessingJobReportWriter;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * Created by paul on 19/06/15.
 */
public class UIMTester {
    private final static String JobCreatorOptions = "job-creator";
    private final static String ReportProcessingOptions = "report-processing";


    private static void printHelp() {
        System.out.println ("How to used the program:");
        System.out.println ("<job-creator|report-processing> uim-tester.conf job-creator-input.conf job-creator-output.conf");
        System.out.println ("report-processing uim-tester.conf job-creator-input.conf job-creator-output.conf moreLogInfo");
    }

    public static void main(String args[]) throws IOException, InterruptedException, TimeoutException,
                                                  ExecutionException {

        boolean printMore = false;
        if ((5 == args.length) && ReportProcessingOptions.equalsIgnoreCase(args[0])) {
            if (!args[4].equalsIgnoreCase("moreLogInfo")) {
                printHelp();
                System.exit(-1);
            }
            else printMore = true;
        }
        else if ( (4 != args.length) || !isAcceptableOption(args[0])) {
            printHelp();
            System.exit(-1);
        }

        final File uimTesterConfigFile = new File(args[1]);
        final File jobCreatorInput = new File(args[2]);
        final File jobCreatorOutput = new File(args[3]);

        if (!uimTesterConfigFile.canRead()) {
            System.out.println("Cannot read uim tester config file: " + uimTesterConfigFile.getAbsolutePath());
            System.exit(-1);
        }

        if (!jobCreatorInput.canRead()) {
            System.out.println("Cannot read from the job creator input file: " + jobCreatorInput.getAbsolutePath());
            System.exit(-1);
        }

        if (!jobCreatorOutput.canWrite()) {
            System.out.println("Cannot write to the job creator output file: " + jobCreatorOutput.getAbsolutePath());
            System.exit(-1);
        }

        final UIMTesterConfig uimTesterConfig = new UIMTesterConfig(uimTesterConfigFile);

        final Mongo mongo = new Mongo(uimTesterConfig.getMongoHost(), uimTesterConfig.getMongoPort());

        if (StringUtils.isNotEmpty(uimTesterConfig.getMongoDBUserName())) {
            final boolean auth =  mongo.getDB("admin").authenticate(uimTesterConfig.getMongoDBUserName(),
                                                                    uimTesterConfig.getMongoDBPassword().toCharArray()
                                                                   );

            if (!auth) {
                System.out.println ("Cannot authenticate to mongo");
                System.exit(-1);
            }
        }

        final Datastore dataStore = new Morphia().createDatastore(mongo, uimTesterConfig.getMongoDBName());
        final HarvesterClient harvesterClient =
                new HarvesterClientImpl(new ProcessingJobDaoImpl(dataStore),
                                        new MachineResourceReferenceDaoImpl(dataStore),
                                        new SourceDocumentProcessingStatisticsDaoImpl(dataStore),
                                        new SourceDocumentReferenceDaoImpl(dataStore),
                                        new SourceDocumentReferenceMetaInfoDaoImpl(dataStore),
                                        new HarvesterClientConfig(uimTesterConfig.getWriteConcern())
                                       );

        final Writer writer = Files.newBufferedWriter(jobCreatorOutput.toPath(), Charset.defaultCharset());

        switch (args[0]) {
            case JobCreatorOptions:
                new JobCreatorTester(harvesterClient,
                                     new JobCreatorTesterOutputWriter(writer)
                                    ).execute(jobCreatorInput);
                break;

            case ReportProcessingOptions:
                new ProcessingJobReport(new ProcessingJobReportRetriever(harvesterClient),
                                        new ProcessingJobReportWriter(writer, printMore)
                                       ).execute(args[2]);
                break;

            default:
                System.out.println("I'm confused !! Don't know what to execute: " + args[0]);
                System.exit(-1);
        }

        System.out.println ("everything was dumped into: " + jobCreatorOutput.getAbsolutePath());
    }

    private static boolean isAcceptableOption (String arg) {
        return JobCreatorOptions.equalsIgnoreCase(arg) ||
               ReportProcessingOptions.equalsIgnoreCase(ReportProcessingOptions);
    }
}
