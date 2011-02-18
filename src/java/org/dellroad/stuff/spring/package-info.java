
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

/**
 * <a href="http://www.springframework.org/">Spring</a> related classes.
 *
 * <p>
 * This package also provides support for the <code>&lt;dellroad-stuff:sql&gt;</code> custom Spring XML element.
 * This element is a shortcut for defining a {@link org.dellroad.stuff.schema.SQLDatabaseAction SQLDatabaseAction}
 * XML bean. This tag supports the {@code split-pattern} attribute, though letting it be optional by providing
 * the a default value of <code>";[ \t\r]*\n\s*"</code>.
 *
 * <p>
 * The SQL script itself can either be nested directly inside the element, or specified as an external Spring
 * resource via the {@code resource} (with optional {@code charset}) attribute.
 *
 * @see <a href="http://www.springframework.org/">Spring Framework</a>
 */
package org.dellroad.stuff.spring;
