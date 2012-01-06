
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

/**
 * Classes supporting automated database schema management.
 *
 * <p>
 * Features include:
 *  <ul>
 *  <li>Automatic initialization of the database schema when the application runs for the first time</li>
 *  <li>Automatic application of database schema updates as needed during each application startup cycle</li>
 *  <li>Automated tracking and constraint-based ordering of schema updates supporting multiple code branches</li>
 *  <li>Integration with <a href="http://www.springframework.org/">Spring</a> allowing simple XML declaration of updates</li>
 *  <li>An <a href="http://ant.apache.org/">ant</a> task that verifies schema update correctness</li>
 *  </ul>
 * </p>
 *
 * <p>
 * See {@link org.dellroad.stuff.schema.SpringSQLSchemaUpdater} for an example of how to declare your
 * {@link javax.sql.DataSource DataSource} and associated schema updates in a Spring application context.
 * </p>
 *
 * <p>
 * Updates may have ordering constraints, and these should be declared explicitly. Once you have done so, then
 * you may safely "cherry pick" individual schema updates for merging into different code branches without worrying
 * whether the schema will get messed up, because any ordering constraint violations will be detected automatically.
 * This verification step is required to detect inconsistencies between the updates and the current code.
 * </p>
 *
 * <p>
 * See DellRoad Stuff's <a href="/svn/trunk/src/build/macros.xml">ant macros</a> for the {@code schemacheck}
 * ant macro that can be used to verify that your delcared schema updates, when applied to the original schema,
 * yield the expected result (which is typically generated automatically by your schema generation tool from
 * your current code). It is also a good idea to compare your generated shema matches to an expected result
 * during each build to detect schema changes caused by e.g., inadvertent changes to model classes.
 * </p>
 *
 * <p>
 * The central classes are {@link org.dellroad.stuff.schema.SQLSchemaUpdater} and
 * {@link org.dellroad.stuff.schema.SpringSQLSchemaUpdater}.
 * </p>
 */
package org.dellroad.stuff.schema;
