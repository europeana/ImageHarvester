package eu.europeana.publisher;

import com.typesafe.config.*;
import eu.europeana.publisher.logging.LoggingComponent;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonController;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.charset.Charset;

public class PublisherDaemon implements Daemon {

    private final org.slf4j.Logger LOG = LoggerFactory.getLogger(this.getClass().getName());

    private Publisher publisher;

    @Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
        LOG.debug(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                "Initialising the publisher daemon");

        String[] args = daemonContext.getArguments();

        String configFilePath;
        if(args.length == 0) {
            configFilePath = "./extra-files/config-files/publishing.conf";
        } else if(1 == args.length) {
            configFilePath = args[0];
        }
        else {
            configFilePath = args[0];

        }

        final File configFile = new File(configFilePath);
        if(!configFile.exists()) {
            LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                    "Config file not found. Exiting.");
            System.exit(-1);
        }

        final Config config = ConfigFactory.parseFileAnySyntax(configFile,
                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.CONF));



        publisher = new Publisher(config);
    }

    @Override
    public void start() throws Exception {
        try {
            publisher.start();
        } catch (Exception e){
            LOG.debug(LoggingComponent
                            .appendAppFields(LoggingComponent.Migrator.PROCESSING, null, null, null),
                    "Stopping publisher while processing batch because of exception.",e);
            destroy();
            System.exit(-1);
        }
    }

    @Override
    public void stop() throws Exception {
        publisher.stop();
    }

    @Override
    public void destroy() {

    }

    public static void main(final String[] args) throws Exception {
        final PublisherDaemon publisherDaemon = new PublisherDaemon();
        publisherDaemon.init(new DaemonContext() {
            @Override
            public DaemonController getController () {
                return null;
            }

            @Override
            public String[] getArguments () {
                return args;
            }
        });
        try {
            publisherDaemon.start();
        }
        finally {
           publisherDaemon.destroy();
        }
    }
}
