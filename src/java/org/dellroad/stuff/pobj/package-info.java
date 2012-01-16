
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

/**
 * Simple XML Persistence Objects (POBJ).
 *
 * <p>
 * This package implements a library for <b>simple</b> persistence of Java objects via XML.
 * It's targeted at Java data structures that are small enough to fit in memory and
 * which are read much more often than they are written, yet still need to be persisted.
 * A good example would be configuration information (e.g., users, groups, rights, etc).
 *
 * <p>
 * Attributes and features:
 * <ul>
 * <li>Able to persist an arbitrary java object graph - you supply the XML (de)serialization strategy</li>
 * <li>Built-in support for making deep copies of the object graph</li>
 * <li>Read access uses natural Java</li>
 * <li>Changes are atomic, serialized, wholesale updates</li>
 * <li>All updates must fully validate</li>
 * <li>Modification serial number allows for optimistic locking</li>
 * <li>Support for listener notifications on update with merge information</li>
 * <li>Support for automated intialization and schema update tracking using {@link org.dellroad.stuff.schema} classes</li>
 * </ul>
 *
 * <p>
 * The primary class is {@link org.dellroad.stuff.pobj.PersistentObject}. Typically this class would be accessed through a
 * {@link org.dellroad.stuff.pobj.PersistentObjectSchemaUpdater} which allows for evolution of the XML schema over time;
 * see {@link org.dellroad.stuff.pobj.SpringPersistentObjectSchemaUpdater} for an example of Spring configuration.
 *
 * @see <a href="http://dellroad-stuff.googlecode.com/">The dellroad-stuff Project</a>
 */
package org.dellroad.stuff.pobj;
