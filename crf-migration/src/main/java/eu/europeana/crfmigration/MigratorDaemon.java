package eu.europeana.crfmigration;

import eu.europeana.crfmigration.logging.LoggingComponent;
import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class MigratorDaemon implements Daemon {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MigratorDaemon.class.getName());

    private Migrator migrator;

    @Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {

        String[] args = daemonContext.getArguments();

        Date dateFilter = null;

        if (1 == args.length) {
            try {
                dateFilter = ISODateTimeFormat.dateTime().parseDateTime(args[0]).toDate();
            } catch (Exception e) {
                LOG.error(LoggingComponent.appendAppFields(LoggingComponent.Migrator.PROCESSING),
                        "The timestamp must respect the ISO 861 format! E.q: yyyy-MM-dd'T'HH:mm:ss.SSSZZ defaulting to begining of time", e);
                dateFilter = DateTime.now().minusYears(20).toDate();
            }
        } else if (args.length > 1) {
            System.out.println("Too many arguments. Please provide only one !");
            System.exit(-1);
        }

        migrator = new Migrator(dateFilter);
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
}
