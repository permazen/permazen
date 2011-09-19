
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.util.List;

/**
 * Spring-enabled {@link SchemaUpdate} supporting updates via SQL statements.
 *
 * <p>
 * Instances can be created succintly using the <code>&lt;dellroad-stuff:sql-update&gt;</code> custom XML element, which works
 * just like <code>&lt;dellroad-stuff:sql&gt;</code> except that it wraps the resulting {@link SQLDatabaseAction}
 * as a delegate inside an instance of this class.
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
 * </p>
 *
 * <p>
 * A multi-statement SQL script is normally treated as a set of individual updates. For example:
 * <blockquote><pre>
 *      <b>&lt;dellroad-stuff:sql-update id="renameColumn"&gt;
 *          ALTER TABLE User ADD newName VARCHAR(64);
 *          ALTER TABLE User SET newName = oldName;
 *          ALTER TABLE User DROP oldName;
 *      &lt;/dellroad-stuff:sql-update&gt;</b>
 * </pre></blockquote>
 * This will create three separate update beans named <code>renameColumn-00001</code>, <code>renameColumn-00002</code>, and
 * <code>renameColumn-00003</code>. You can disable this behavior by adding the attribute <code>single-action="true"</code>,
 * in which case all three of the statements will be executed together in the same transaction and recorded under the name
 * <code>renameColumn</code>; this means that they must all complete successfully or you could end up with a partially
 * completed update.
 * </p>
 *
 * <p>
 * Note that if the nested SQL script only contains one SQL statement, any <code>single-action</code> attribute is
 * ignored and the bean's given name (e.g., <code>renameColumn</code>) is always used as the name of the single update.
 * </p>
 */
public class SpringSQLSchemaUpdate extends AbstractSpringSchemaUpdate {

    private SQLDatabaseAction action;

    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        if (this.action == null)
            throw new Exception("no DatabaseAction configured");
    }

    /**
     * Configure the {@link SQLDatabaseAction}. This is a required property.
     *
     * @see DatabaseAction
     */
    public void setSQLDatabaseAction(SQLDatabaseAction action) {
        this.action = action;
    }

    @Override
    public List<DatabaseAction> getDatabaseActions() {
        return this.action.split();
    }
}

