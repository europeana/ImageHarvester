package eu.europeana.harvester.cluster;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.commons.daemon.DaemonInitException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MasterDaemon implements Daemon {

    private static final Logger LOG = LogManager.getLogger(MasterDaemon.class.getName());

    private Master master;

    @Override
    public void init(DaemonContext daemonContext) throws DaemonInitException, Exception {
        LOG.debug("Initializing master");

        String[] args = daemonContext.getArguments();
        master = new Master(args);
        master.init();
    }

    @Override
    public void start() throws Exception {
        LOG.debug("Starting master");

        master.start();
    }

    @Override
    public void stop() throws Exception {
        LOG.debug("Stopping master");

        master.getActorSystem().shutdown();
    }

    @Override
    public void destroy() {
        LOG.debug("Destroying master");
    }
}
