package de.jbellmann.tomcat.cassandra.hector;

import de.jbellmann.tomcat.cassandra.CassandraManager;
import de.jbellmann.tomcat.cassandra.CassandraTemplate;

/**
 * 
 * @author Joerg Bellmann
 *
 */
public class HectorCassandraManager extends CassandraManager {

    private static final String INFO = HectorCassandraManager.class.getName() + "/1.0";

    @Override
    public CassandraTemplate createCassandraTemplate() {
        return new HectorCassandraTemplate();
    }

    @Override
    public String getName() {
        return "hector-cassandra-manager";
    }

    @Override
    public String getInfo() {
        return INFO;
    }

}
