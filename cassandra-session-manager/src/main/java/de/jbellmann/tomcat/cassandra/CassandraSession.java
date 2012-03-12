package de.jbellmann.tomcat.cassandra;

import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;

import javax.servlet.http.HttpSession;

import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;
import org.apache.commons.lang.StringUtils;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

/**
 * Attribute-access will be forwarded to {@link CassandraTemplate}.
 * 
 * {@link CassandraTemplate} is available {@link CassandraManager}.
 * 
 * @author Joerg Bellmann
 *
 */
public class CassandraSession extends StandardSession {

    private static final long serialVersionUID = 1L;

    private final Log log = LogFactory.getLog(CassandraSession.class);

    public CassandraSession(Manager manager) {
        super(manager);
    }

    public void setIdInternal(String id) {
        this.id = id;
    }

    @Override
    public void setCreationTime(long time) {
        if (StringUtils.isBlank(getId())) {
            log.warn("-- invoked when id was blank");
            return;
        }
        getCassandraSessionOperations().setCreationTime(getId(), time);
    }

    public void setLastAccessedTime(long time) {
        if (StringUtils.isBlank(getId())) {
            log.warn("-- invoked when id was blank");
            return;
        }
        getCassandraSessionOperations().setLastAccessedTime(getId(), time);
    }

    @Override
    public long getLastAccessedTime() {
        if (StringUtils.isBlank(getId())) {
            log.warn("-- invoked when id was blank");
            return -1;
        }
        return getCassandraSessionOperations().getLastAccessedTime(getId());
    }

    @Override
    public long getLastAccessedTimeInternal() {
        return super.getLastAccessedTime();
    }

    @Override
    public HttpSession getSession() {
        return new CassandraSessionFacade(this);
    }

    @Override
    public boolean isValid() {
        if (this.expiring) {
            return true;
        }

        if (!this.isValid) {
            return false;
        }

        long lastAccessedTime = getLastAccessedTime();
        if (lastAccessedTime < 0) {
            this.isValid = false;
            return false;
        }

        if (maxInactiveInterval >= 0) {
            long timeNow = System.currentTimeMillis();
            int timeIdle = (int) ((timeNow - lastAccessedTime) / 1000L);
            if (timeIdle >= maxInactiveInterval) {
                expire(true);
            }
        }

        return this.isValid;
    }

    @Override
    public long getCreationTime() {
        if (StringUtils.isBlank(getId())) {
            log.warn("-- invoked when id was blank");
            return -1;
        }
        return getCassandraSessionOperations().getCreationTime(getId());
    }

    @Override
    public void removeAttribute(String name) {
        if (StringUtils.isBlank(getId())) {
            log.warn("-- invoked when id was blank");
            return;
        }
        getCassandraSessionOperations().removeAttribute(getId(), name);
    }

    @Override
    public void removeAttribute(String name, boolean notify) {
        removeAttribute(name);
    }

    @Override
    public void setAttribute(String name, Object value) {
        if (StringUtils.isBlank(getId())) {
            log.warn("-- invoked when id was blank");
            return;
        }
        getCassandraSessionOperations().setAttribute(getId(), name, value);
    }

    @Override
    public void setAttribute(String name, Object value, boolean notify) {
        setAttribute(name, value);
    }

    @Override
    public Object getAttribute(String name) {
        if (StringUtils.isBlank(getId())) {
            log.warn("-- invoked when id was blank");
            return null;
        }
        return getCassandraSessionOperations().getAttribute(getId(), name);
    }

    @Override
    public Enumeration<String> getAttributeNames() {
        return Collections.enumeration(Arrays.asList(keys()));
    }

    @Override
    protected String[] keys() {
        if (StringUtils.isBlank(getId())) {
            log.warn("-- invoked when id was blank");
            return new String[0];
        }
        return getCassandraSessionOperations().keys(getId());
    }

    CassandraOperations getCassandraSessionOperations() {
        return ((CassandraManager) manager).getCassandraOperations();
    }

    /**
     * Return a string representation of this object.
     */
    @Override
    public String toString() {

        StringBuilder sb = new StringBuilder();
        sb.append("CassandraSession[");
        sb.append(id);
        sb.append("]");
        return (sb.toString());

    }
}
