
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Spring-enabled version of {@link DelegatingSchemaUpdate}.
 *
 * <p>
 * This class provides the combined functionality of {@link DelegatingSchemaUpdate} and {@link AbstractSpringSchemaUpdate}.
 *
 * <p>
 * When using Spring and setting the {@link #setDatabaseAction database action} to a {@link SQLDatabaseAction}, beans
 * can be created succintly using the <code>&lt;dellroad-stuff:sql-update&gt;</code> custom XML element, which works
 * just like <code>&lt;dellroad-stuff:sql&gt;</code> except that it wraps the resulting {@link SQLDatabaseAction}
 * inside an instance of this class as the delegate.
 *
 * <p>
 * For example:
 * <blockquote><pre>
 *  &lt;beans xmlns="http://www.springframework.org/schema/beans"
 *    <b>xmlns:dellroad-stuff="http://dellroad-stuff.googlecode.com/schema/dellroad-stuff"</b>
 *    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
 *    xsi:schemaLocation="
 *      http://www.springframework.org/schema/beans
 *        http://www.springframework.org/schema/beans/spring-beans-3.0.xsd
 *      <b>http://dellroad-stuff.googlecode.com/schema/dellroad-stuff
 *        http://dellroad-stuff.googlecode.com/svn/wiki/schemas/dellroad-stuff-1.0.xsd</b>"&gt;
 *
 *      &lt;!-- Schema update to add the 'phone' column to the 'User' table --&gt;
 *      <b>&lt;dellroad-stuff:sql-update id="addPhone"&gt;ALTER TABLE User ADD phone VARCHAR(64)&lt;/dellroad-stuff:sql-update&gt;</b>
 *
 *      &lt;!-- Schema update to run some complicated external SQL script --&gt;
 *      <b>&lt;dellroad-stuff:sql-update id="majorChanges" depends-on="addPhone" resource="classpath:majorChanges.sql"/&gt;</b>
 *
 *      &lt;!-- more beans... --&gt;
 *
 *  &lt;/beans&gt;
 * </pre></blockquote>
 */
public class SpringDelegatingSchemaUpdate extends AbstractSpringSchemaUpdate {

    private DatabaseAction action;

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        if (this.action == null)
            throw new Exception("no DatabaseAction configured");
    }

    /**
     * Configure the {@link DatabaseAction}. This is a required property.
     *
     * @see DatabaseAction
     */
    public void setDatabaseAction(DatabaseAction action) {
        this.action = action;
    }

    @Override
    public void apply(Connection c) throws SQLException {
        this.action.apply(c);
    }
}

