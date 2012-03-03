package de.jbellmann.tomcat.cassandra;

import java.util.Enumeration;
import java.util.List;

public interface CassandraOperations {

    //    Object execute(CassandraCallback callback) throws CassandraCallbackException;

    // operations needed in Session

    long getCreationTime(String sessionId);

    void setCreationTime(String sessionId, long time);

    long getLastAccessedTime(String sessionId);

    void setLastAccessedTime(String sessionId, long time);

    Object getAttribute(String sessionId, String name);

    void setAttribute(String sessionId, String name, Object value);

    void removeAttribute(String sessionId, String name);

    String[] keys(String sessionId);

    Enumeration<String> getAttributeNames(String sessionId);

    // operations needed in Manager

    List<String> findSessionKeys();

    //    void addSession(Session session);

    void removeSession(String sessionId);

}
