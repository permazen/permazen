
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import org.dellroad.stuff.schema.SpringSQLSchemaUpdate;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.ManagedSet;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * Parses <code>&lt;dellroad-stuff:sql-update&gt;</code> tags.
 *
 * @see SpringSQLSchemaUpdate
 */
class SQLUpdateBeanDefinitionParser extends AbstractBeanDefinitionParser {

    private static final String SINGLE_ACTION_ATTRIBUTE = "single-action";

    @Override
    protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {

        // Verify there is an "id" attribute
        final String id = element.getAttribute(ID_ATTRIBUTE);
        if (!StringUtils.hasText(id)) {
            this.error(element, parserContext, "<" + element.getTagName() + "> beans must have an \""
              + ID_ATTRIBUTE + "\" attribute that provides a unique name for the update");
        }

        // Create bean definition for a SpringSQLSchemaUpdate bean
        AbstractBeanDefinition update = this.createBeanDefinition(SpringSQLSchemaUpdate.class, element, parserContext);
        this.parseStandardAttributes(update, element, parserContext);

        // Parse this element like a <dellroad-stuff:sql> element and then make that bean my delegate
        update.getPropertyValues().add("SQLDatabaseAction", new SQLBeanDefinitionParser(true).parse(element, parserContext));

        // Set required predecessors (if any)
        String[] predecessors = update.getDependsOn();
        if (predecessors != null) {
            final Object source = parserContext.extractSource(element);
            ManagedSet<Object> predecessorSet = new ManagedSet<Object>(predecessors.length);
            predecessorSet.setSource(source);
            for (String predecessor : predecessors) {
                RuntimeBeanReference beanReference = new RuntimeBeanReference(predecessor, false);
                beanReference.setSource(source);
                predecessorSet.add(beanReference);
            }
            update.getPropertyValues().add("requiredPredecessors", predecessorSet);
        }

        // Set single action property
        final String singleAction = element.getAttribute(SINGLE_ACTION_ATTRIBUTE);
        if (StringUtils.hasText(singleAction))
            update.getPropertyValues().add("singleAction", Boolean.valueOf(singleAction));

        // Done
        return update;
    }
}

