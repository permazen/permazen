
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import org.dellroad.stuff.schema.SpringDelegatingSchemaUpdate;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;

/**
 * Parses <code>&lt;dellroad-stuff:sql-update&gt;</code> tags.
 */
class SQLUpdateBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    @Override
    protected Class<SpringDelegatingSchemaUpdate> getBeanClass(Element element) {
        return SpringDelegatingSchemaUpdate.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
        builder.addPropertyValue("databaseAction", new SQLBeanDefinitionParser(true).parse(element, parserContext));
    }
}

