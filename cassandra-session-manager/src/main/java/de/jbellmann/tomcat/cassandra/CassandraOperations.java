/**
 * Copyright 2012 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.jbellmann.tomcat.cassandra;

import java.util.Enumeration;
import java.util.List;

/**
 * 
 * @author Joerg Bellmann
 *
 */
public interface CassandraOperations {

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

    void addSession(String sessionId);

    void removeSession(String sessionId);

}
