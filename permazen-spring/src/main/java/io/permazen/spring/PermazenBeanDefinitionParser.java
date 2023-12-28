
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.spring;

import io.permazen.Permazen;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Parses <code>&lt;permazen:permazen&gt;</code> tags.
 *
 * @see io.permazen.spring
 */
class PermazenBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    private static final String KVSTORE_ATTRIBUTE = "kvstore";
    private static final String ENCODING_REGISTRY_ATTRIBUTE = "encoding-registry";

    @Override
    protected Class<Permazen> getBeanClass(Element element) {
        return Permazen.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        // Get KVDatabase bean name (optional)
        if (element.hasAttribute(KVSTORE_ATTRIBUTE))
            builder.addPropertyReference("KVStore", element.getAttribute(KVSTORE_ATTRIBUTE));

        // Get EncodingRegistry (optional)
        if (element.hasAttribute(ENCODING_REGISTRY_ATTRIBUTE))
            builder.addPropertyValue("encodingRegistry", element.getAttribute(ENCODING_REGISTRY_ATTRIBUTE));

        // Construct PermazenFactoryBean bean definition
        builder.getRawBeanDefinition().setBeanClass(PermazenFactoryBean.class);

        // Look for nested <scan-classes>
        final NodeList nodeList = element.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (!(nodeList.item(i) instanceof Element))
                continue;
            final Element child = (Element)nodeList.item(i);
            if (!PermazenNamespaceHandler.PERMAZEN_NAMESPACE_URI.equals(child.getNamespaceURI()))
                continue;
            if (child.getLocalName().equals(PermazenNamespaceHandler.SCAN_CLASSES_TAG))
                builder.addPropertyValue("modelClasses", new ScanClassesBeanDefinitionParser().parse(child, parserContext));
            else {
                parserContext.getReaderContext().fatal("unsupported <" + child.getTagName() + "> element found"
                  + " inside <" + element.getTagName() + "> element", parserContext.extractSource(element));
                return;
            }
        }
    }
}
