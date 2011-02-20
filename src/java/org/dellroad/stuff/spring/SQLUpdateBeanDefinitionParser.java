
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import org.dellroad.stuff.schema.SpringDelegatingSchemaUpdate;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionValidationException;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Attr;
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

        // Verify there is an "id" attribute
        Attr attr = element.getAttributeNodeNS(null, "id");
        if (attr == null) {
            throw new BeanDefinitionValidationException("<dellroad-stuff:" + DellRoadStuffNamespaceHandler.SQL_UPDATE_ELEMENT_NAME
              + "> beans must have an \"id\" attribute that provides the unique name of the update");
        }

        // Parse this element like a <dellroad-stuff:sql> element and then make that bean my delegate
        builder.addPropertyValue("databaseAction", new SQLBeanDefinitionParser(
          DellRoadStuffNamespaceHandler.SQL_UPDATE_ELEMENT_NAME, true).parse(element, parserContext));
    }
}

