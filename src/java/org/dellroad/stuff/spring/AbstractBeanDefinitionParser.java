
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.BeanDefinitionParserDelegate;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.core.env.StandardEnvironment;
import org.w3c.dom.Element;

/**
 * Support superclass for {@link org.springframework.beans.factory.xml.BeanDefinitionParser} implementations.
 */
public abstract class AbstractBeanDefinitionParser extends org.springframework.beans.factory.xml.AbstractBeanDefinitionParser {

    /**
     * Create a new bean definition.
     *
     * @param beanClass bean type
     * @param element the element that the new bean definition is associated with
     * @param parserContext parser context
     */
    protected AbstractBeanDefinition createBeanDefinition(Class<?> beanClass, Element element, ParserContext parserContext) {
        BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition();
        builder.getRawBeanDefinition().setBeanClass(beanClass);
        builder.getRawBeanDefinition().setSource(parserContext.extractSource(element));
        if (parserContext.isNested())
            builder.setScope(parserContext.getContainingBeanDefinition().getScope());
        if (parserContext.isDefaultLazyInit())
            builder.setLazyInit(true);
        return builder.getBeanDefinition();
    }

    /**
     * Parse the standard Spring bean attributes such as {@code scope}, {@code depends-on}, {@code abstract}, etc.
     *
     * @param beanDefinition bean definition to modify
     * @param element the XML element on which to look for attributes
     * @param parserContext parser context
     */
    protected void parseStandardAttributes(AbstractBeanDefinition beanDefinition, Element element, ParserContext parserContext) {
        BeanDefinitionParserDelegate delegate = new BeanDefinitionParserDelegate(parserContext.getReaderContext(),
          new StandardEnvironment());
        delegate.parseBeanDefinitionAttributes(element, "", null, beanDefinition);
    }

    /**
     * Report an error and throw an exception.
     */
    protected void error(Element element, ParserContext parserContext, String message) {
        parserContext.getReaderContext().fatal(message, parserContext.extractSource(element));
    }
}

