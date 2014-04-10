
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.spring;

import java.util.ArrayList;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ListFactoryBean;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.beans.factory.xml.XmlReaderContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScanBeanDefinitionParser;
import org.springframework.core.env.Environment;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Parses <code>&lt;jsimpledb:scan-classpath&gt;</code> tags.
 */
class ScanClassPathBeanDefinitionParser extends ComponentScanBeanDefinitionParser {

    private static final String BASE_PACKAGE_ATTRIBUTE = "base-package";
    private static final String RESOURCE_PATTERN_ATTRIBUTE = "resource-pattern";
    private static final String USE_DEFAULT_FILTERS_ATTRIBUTE = "use-default-filters";
    private static final String EXCLUDE_FILTER_ELEMENT = "exclude-filter";
    private static final String INCLUDE_FILTER_ELEMENT = "include-filter";

    @Override
    public BeanDefinition parse(Element element, ParserContext parserContext) {

        // Scan for @JSimpleClass and @JSimpleFieldType-annotated classes
        final String[] basePackages = StringUtils.tokenizeToStringArray(element.getAttribute(BASE_PACKAGE_ATTRIBUTE),
          ConfigurableApplicationContext.CONFIG_LOCATION_DELIMITERS);
        final ScanClassPathClassScanner scanner = this.createScanner(parserContext, element);
        final ArrayList<String> classNames = scanner.scanForClasses(basePackages);

        // Build <util:list> equivalent bean
        final BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition();
        builder.getRawBeanDefinition().setBeanClass(ListFactoryBean.class);
        builder.addPropertyValue("sourceList", classNames);
        builder.addPropertyValue("targetListClass", ClassList.class);       // forces automatic String -> Class conversion
        builder.getRawBeanDefinition().setSource(parserContext.extractSource(element));
        if (parserContext.isNested())
            builder.setScope(parserContext.getContainingBeanDefinition().getScope());
        if (parserContext.isDefaultLazyInit())
            builder.setLazyInit(true);
        return builder.getBeanDefinition();
    }

    protected ScanClassPathClassScanner createScanner(ParserContext parserContext, Element element) {
        final XmlReaderContext readerContext = parserContext.getReaderContext();
        final Environment environment = parserContext.getDelegate().getEnvironment();
        boolean useDefaultFilters = true;
        if (element.hasAttribute(USE_DEFAULT_FILTERS_ATTRIBUTE))
            useDefaultFilters = Boolean.valueOf(element.getAttribute(USE_DEFAULT_FILTERS_ATTRIBUTE));
        final ScanClassPathClassScanner scanner = new ScanClassPathClassScanner(useDefaultFilters, environment);
        scanner.setResourceLoader(readerContext.getResourceLoader());
        if (element.hasAttribute(RESOURCE_PATTERN_ATTRIBUTE))
            scanner.setResourcePattern(element.getAttribute(RESOURCE_PATTERN_ATTRIBUTE));
        this.parseTypeFilters(element, scanner, readerContext, parserContext);
        return scanner;
    }

    protected void parseTypeFilters(Element element, ScanClassPathClassScanner scanner,
      XmlReaderContext readerContext, ParserContext parserContext) {

        // Parse exclude and include filter elements.
        final ClassLoader classLoader = scanner.getResourceLoader().getClassLoader();
        final NodeList nodeList = element.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            final Node node = nodeList.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE)
                continue;
            final String localName = parserContext.getDelegate().getLocalName(node);
            final Element childElement = (Element)node;
            try {
                if (INCLUDE_FILTER_ELEMENT.equals(localName))
                    scanner.addIncludeFilter(this.createTypeFilter(childElement, classLoader));
                if (EXCLUDE_FILTER_ELEMENT.equals(localName))
                    scanner.addExcludeFilter(this.createTypeFilter(childElement, classLoader));
            } catch (Exception e) {
                readerContext.error(e.getMessage(), readerContext.extractSource(element), e.getCause());
            }
        }
    }

// ClassList

    @SuppressWarnings("serial")
    public static class ClassList extends ArrayList<Class<?>> {
    }
}

