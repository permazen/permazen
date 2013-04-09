
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.servlet;

import java.sql.Driver;
import java.sql.DriverManager;
import java.util.Enumeration;

import javax.servlet.ServletContextEvent;

import org.dellroad.stuff.spring.ThreadLocalContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.util.IntrospectorCleanupListener;

/**
 * Clears references that can cause container memory leaks on Web container class unloading.
 *
 * @see <a href="http://opensource.atlassian.com/confluence/spring/pages/viewpage.action?pageId=2669">Spring Wiki Discussion of
 *      ClassLoader Memory Leaks</a>
 * @see <a href="http://bugs.mysql.com/bug.php?id=65909">MySQL Bug #65909</a>
 * @since 1.0.49
 */
public class ContextCleanupListener extends IntrospectorCleanupListener {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * Unregisters any JDBC drivers registered under the current class loader after
     * first invoking the superclass version of this method.
     *
     * <p>
     * Also resets the {@link ThreadLocalContext} singleton instance
     * so Tomcat won't complain about the lingering ThreadLocal variables.
     * </p>
     */
    @Override
    public void contextDestroyed(ServletContextEvent event) {
        try {
            super.contextDestroyed(event);

            // Get my class loader
            final ClassLoader thisClassLoader = this.getClass().getClassLoader();

            // Work around MySQL bug http://bugs.mysql.com/bug.php?id=65909
            try {
                Class.forName("com.mysql.jdbc.AbandonedConnectionCleanupThread", false,
                  thisClassLoader).getMethod("shutdown").invoke(null);
            } catch (Exception e) {
                // ignore
            }

            // Unregister JDBC drivers
            for (Enumeration<Driver> e = DriverManager.getDrivers(); e.hasMoreElements(); ) {
                final Driver driver = e.nextElement();
                if (driver.getClass().getClassLoader() == thisClassLoader)
                    DriverManager.deregisterDriver(driver);
            }

            // Reset the ThreadLocalContext singleton instance to make Tomcat happy
            ThreadLocalContext.setInstance(new ThreadLocalContext());
        } catch (Throwable e) {
            log.error("exception cleaning up servlet context", e);
        }
    }
}
