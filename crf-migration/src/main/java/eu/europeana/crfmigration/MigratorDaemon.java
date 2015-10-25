package eu.europeana.crfmigration;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.slf4j.LoggerFactory;

public class MigratorDaemon implements Daemon {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(MigratorDaemon.class.getName());

    private Migrator migrator;

    @Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {

        migrator = new Migrator();
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
