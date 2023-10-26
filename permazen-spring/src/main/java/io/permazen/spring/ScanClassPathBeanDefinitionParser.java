
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

import java.util.ArrayList;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.beans.factory.xml.XmlReaderContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScanBeanDefinitionParser;
import org.springframework.core.type.filter.TypeFilter;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Support superclass for XML bean definition parsers.
 */
abstract class ScanClassPathBeanDefinitionParser extends ComponentScanBeanDefinitionParser {

    private static final String BASE_PACKAGE_ATTRIBUTE = "base-package";
    private static final String RESOURCE_PATTERN_ATTRIBUTE = "resource-pattern";
    private static final String USE_DEFAULT_FILTERS_ATTRIBUTE = "use-default-filters";
    private static final String EXCLUDE_FILTER_ELEMENT = "exclude-filter";
    private static final String INCLUDE_FILTER_ELEMENT = "include-filter";

    @Override
    public BeanDefinition parse(Element element, ParserContext parserContext) {

        // Get context info
        final XmlReaderContext readerContext = parserContext.getReaderContext();
        final ClassLoader classLoader = readerContext.getResourceLoader().getClassLoader();

        // Build bean definition
        final BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(this.getBeanClass());
        builder.getRawBeanDefinition().setSource(parserContext.extractSource(element));
        if (parserContext.isNested())
            builder.setScope(parserContext.getContainingBeanDefinition().getScope());
        if (parserContext.isDefaultLazyInit())
            builder.setLazyInit(true);
        builder.addPropertyValue("basePackages", StringUtils.tokenizeToStringArray(
          element.getAttribute(BASE_PACKAGE_ATTRIBUTE), ConfigurableApplicationContext.CONFIG_LOCATION_DELIMITERS));
        if (element.hasAttribute(USE_DEFAULT_FILTERS_ATTRIBUTE))
            builder.addPropertyValue("useDefaultFilters", element.getAttribute(USE_DEFAULT_FILTERS_ATTRIBUTE));
        if (element.hasAttribute(RESOURCE_PATTERN_ATTRIBUTE))
            builder.addPropertyValue("resourcePattern", element.getAttribute(RESOURCE_PATTERN_ATTRIBUTE));

        // Parse exclude and include filter elements
        final ArrayList<TypeFilter> includeFilters = new ArrayList<>();
        final ArrayList<TypeFilter> excludeFilters = new ArrayList<>();
        final NodeList nodeList = element.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            final Node node = nodeList.item(i);
            if (node.getNodeType() != Node.ELEMENT_NODE)
                continue;
            final String localName = parserContext.getDelegate().getLocalName(node);
            final Element childElement = (Element)node;
            try {
                if (INCLUDE_FILTER_ELEMENT.equals(localName))
                    includeFilters.add(this.createTypeFilter(childElement, classLoader, parserContext));
                if (EXCLUDE_FILTER_ELEMENT.equals(localName))
                    excludeFilters.add(this.createTypeFilter(childElement, classLoader, parserContext));
            } catch (Exception e) {
                readerContext.error(e.getMessage(), readerContext.extractSource(element), e.getCause());
            }
        }
        builder.addPropertyValue("includeFilters", includeFilters);
        builder.addPropertyValue("excludeFilters", excludeFilters);

        // Done
        return builder.getBeanDefinition();
    }

    abstract Class<?> getBeanClass();
}
