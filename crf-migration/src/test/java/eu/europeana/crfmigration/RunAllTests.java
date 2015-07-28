package eu.europeana.crfmigration;

import eu.europeana.crfmigration.dao.MigratorEuropeanaDaoTest;
import eu.europeana.crfmigration.dao.MigratorHarvesterDaoTest;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Created by salexandru on 27.07.2015.
 */

@RunWith(Suite.class)
@Suite.SuiteClasses({MigratorEuropeanaDaoTest.class, MigratorHarvesterDaoTest.class, MigrationManagerTest.class})
public class RunAllTests {}
