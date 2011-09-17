
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;

/**
 * Spring {@link org.springframework.beans.factory.xml.NamespaceHandler NamespaceHandler}
 * for the <code>dellroad-stuff</code> XML namespace.
 *
 * <p>
 * This adds support for the <code>&lt;dellroad-stuff:sql/&gt;</code> XML tag, which defines a
 * {@link org.dellroad.stuff.schema.SQLDatabaseAction} bean, and the <code>&lt;dellroad-stuff:sql-update/&gt;</code> XML tag,
 * which wraps the same thing in a {@link org.dellroad.stuff.schema.SpringSQLSchemaUpdate} bean.
 */
public class DellRoadStuffNamespaceHandler extends NamespaceHandlerSupport {

    public static final String NAMESPACE_URI = "http://dellroad-stuff.googlecode.com/schema/dellroad-stuff";

    public static final String SQL_ELEMENT_NAME = "sql";
    public static final String SQL_UPDATE_ELEMENT_NAME = "sql-update";

    public void init() {
        registerBeanDefinitionParser(SQL_ELEMENT_NAME, new SQLBeanDefinitionParser());
        registerBeanDefinitionParser(SQL_UPDATE_ELEMENT_NAME, new SQLUpdateBeanDefinitionParser());
    }
}

