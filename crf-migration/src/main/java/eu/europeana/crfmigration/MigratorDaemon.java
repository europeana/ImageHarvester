package eu.europeana.crfmigration;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import eu.europeana.crfmigration.domain.GraphiteReporterConfig;
import eu.europeana.crfmigration.domain.MigratorConfig;
import eu.europeana.crfmigration.logging.LoggingComponent;
import eu.europeana.harvester.domain.MongoConfig;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonController;
import org.apache.commons.daemon.DaemonInitException;
import org.joda.time.DateTime;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MigratorDaemon implements Daemon {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MigratorDaemon.class.getName());
    private static final SimpleDateFormat parserSDF=new SimpleDateFormat("yyyy.MM.dd-HH:mm:ss");

    private Migrator migrator;

    @Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
        LOG.info(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                "Initialising the migrator daemon");

        String[] args = daemonContext.getArguments();

        final MigratorConfig migratorConfig = MigratorConfig.loadFromConfigFilePath(args[0]);

        migrator = new Migrator(migratorConfig);
    }

    @Override
    public void start() throws Exception {
        migrator.start();
    }

    @Override
    public void stop() throws Exception {
        migrator.stop();
    }

    @Override
    public void destroy() {

    }

    public static void main(final String[] args) throws Exception {
        final MigratorDaemon migratorDaemon = new MigratorDaemon();
        migratorDaemon.init(new DaemonContext() {
            @Override
            public DaemonController getController() {
                return null;
            }

            @Override
            public String[] getArguments() {
                return args;
            }
        });
        try {
            migratorDaemon.start();
        }
        finally {
            migratorDaemon.destroy();
        }
    }

}
