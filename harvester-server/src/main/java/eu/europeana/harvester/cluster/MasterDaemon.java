package eu.europeana.harvester.cluster;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MasterDaemon implements Daemon {

    private static final Logger LOG = LogManager.getLogger(MasterDaemon.class.getName());

    private Master master;

    @Override
    public void init(DaemonContext daemonContext) throws Exception {
        //LOG.info("Initializing master");

        String[] args = daemonContext.getArguments();
        master = new Master(args);
        master.init();
    }

    @Override
    public void start() throws Exception {
        //LOG.info("Starting master");

        master.start();
    }

    @Override
    public void stop() throws Exception {
        //LOG.info("Stopping master");

        master.getActorSystem().shutdown();
    }

    @Override
    public void destroy() {
        //LOG.info("Destroying master");
    }
}
