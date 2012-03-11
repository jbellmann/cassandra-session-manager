package de.jbellmann.tomcat.cassandra;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class StringTest {

    @Test
    public void isNullEmpty() {
        String id = null;
        Assert.assertTrue(StringUtils.isBlank(id));

    }

}
