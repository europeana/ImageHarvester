package eu.europeana.harvester;

import eu.europeana.harvester.cluster.slave.RetrieveAndProcessActorTest;
import eu.europeana.harvester.cluster.slave.downloading.*;
import eu.europeana.harvester.cluster.slave.processing.SlaveProcessorTest;
import eu.europeana.harvester.cluster.slave.processing.color.ColorExtractorTest;
import eu.europeana.harvester.cluster.slave.processing.metainfo.MediaMetaInfoTest;
import eu.europeana.harvester.cluster.slave.processing.thumbnail.ThumbnailGeneratorTest;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Created by salexandru on 29.07.2015.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({SlaveDownloaderTest.class, SlaveLinkCheckerTest.class, TimeWindowCounterTest.class,
                     ColorExtractorTest.class, MediaMetaInfoTest.class, ThumbnailGeneratorTest.class,
                     SlaveProcessorTest.class, RetrieveAndProcessActorTest.class})
public class RunAllTests {
    @Rule
    public TestRule watcher = new TestWatcher() {
        @Override
        protected void starting(Description description) {
            System.out.println("Starting test: " + description.getMethodName());
        }

        @Override
        protected  void finished(Description description) {
            System.out.println("Stopping test:" + description.getMethodName());
        }
    };
}
