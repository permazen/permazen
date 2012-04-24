
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * Parses <code>&lt;dellroad-stuff:post-completion&gt;</code> tags.
 */
class PostCompletionBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    public static final String EXECUTOR_ATTRIBUTE = "executor";

    @Override
    protected String getBeanClassName(Element element) {
        return this.getClass().getPackage().getName() + ".PostCompletionAspect";
    }

    @Override
    protected boolean shouldGenerateIdAsFallback() {
        return true;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        // Configure factory method
        builder.setFactoryMethod("aspectOf");

        // Set role
        builder.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);

        // Set the executor
        String executorName = element.getAttribute(EXECUTOR_ATTRIBUTE);
        if (!StringUtils.hasText(executorName)) {
            parserContext.getReaderContext().fatal("<" + element.getTagName() + "> requires an \""
              + EXECUTOR_ATTRIBUTE + "\" attribute", parserContext.extractSource(element));
        }
        builder.addPropertyReference("executor", executorName);
    }
}

