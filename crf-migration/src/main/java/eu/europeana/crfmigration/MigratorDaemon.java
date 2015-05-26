package eu.europeana.crfmigration;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.format.ISODateTimeFormat;

import java.util.Date;

public class MigratorDaemon implements Daemon {

    private static final Logger LOG = LogManager.getLogger(MigratorDaemon.class.getName());

    private Migrator migrator;

    @Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
        LOG.info("Initializing migrator");

        String[] args = daemonContext.getArguments();

        Date dateFilter = null;

        if (1 == args.length) {
            try {
                dateFilter = ISODateTimeFormat.dateTime().parseDateTime(args[0]).toDate();
            } catch (Exception e) {
                LOG.error("The timestamp must respect the ISO 861 format! E.q: yyyy-MM-dd'T'HH:mm:ss.SSSZZ ", e);
                System.exit(-1);
            }
        } else if (args.length > 1) {
            System.out.println("Too many arguments. Please provide only one !");
            System.exit(-1);
        }

        migrator = new Migrator(dateFilter);
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting migrator");

        migrator.start();
    }

    @Override
    public void stop() throws Exception {
        LOG.info("Stopping migrator");

        migrator.stop();
    }

    @Override
    public void destroy() {
        LOG.info("Destroying migrator");
    }
}
