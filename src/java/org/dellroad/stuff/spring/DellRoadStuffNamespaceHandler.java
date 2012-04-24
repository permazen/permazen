
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
 * This adds support for the following Spring custom XML tags:
 * <ul>
 *  <li><code>&lt;dellroad-stuff:sql/&gt;</code>, which defines a {@link org.dellroad.stuff.schema.SQLCommandList} bean</li>
 *  <li><code>&lt;dellroad-stuff:sql-update/&gt;</code>, which wraps a {@link org.dellroad.stuff.schema.SQLCommandList}
 *  in a {@link org.dellroad.stuff.schema.SpringSQLSchemaUpdate} bean</li>
 *  <li><code>&lt;dellroad-stuff:post-completion/&gt;</code>, which activates the
 *  {@link PostCompletionSupport @PostCompletionSupport} annotation</li>
 * </ul>
 */
public class DellRoadStuffNamespaceHandler extends NamespaceHandlerSupport {

    public static final String NAMESPACE_URI = "http://dellroad-stuff.googlecode.com/schema/dellroad-stuff";

    public static final String SQL_ELEMENT_NAME = "sql";
    public static final String SQL_UPDATE_ELEMENT_NAME = "sql-update";
    public static final String POST_COMPLETION_ELEMENT_NAME = "post-completion";

    @Override
    public void init() {
        this.registerBeanDefinitionParser(SQL_ELEMENT_NAME, new SQLBeanDefinitionParser());
        this.registerBeanDefinitionParser(SQL_UPDATE_ELEMENT_NAME, new SQLUpdateBeanDefinitionParser());
        this.registerBeanDefinitionParser(POST_COMPLETION_ELEMENT_NAME, new PostCompletionBeanDefinitionParser());
    }
}

