
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
 * Parses <code>&lt;jsimpledb:jsimpledb&gt;</code> tags.
 *
 * @see io.permazen.spring
 */
class PermazenBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    private static final String KVSTORE_ATTRIBUTE = "kvstore";
    private static final String SCHEMA_VERSION_ATTRIBUTE = "schema-version";
    private static final String STORAGE_ID_GENERATOR_ATTRIBUTE = "storage-id-generator";
    private static final String AUTO_GENERATE_STORAGE_IDS_ATTRIBUTE = "auto-generate-storage-ids";

    @Override
    protected Class<Permazen> getBeanClass(Element element) {
        return Permazen.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        // Get KVDatabase bean name (optional)
        if (element.hasAttribute(KVSTORE_ATTRIBUTE))
            builder.addPropertyReference("KVStore", element.getAttribute(KVSTORE_ATTRIBUTE));

        // Get schema version (optional)
        if (element.hasAttribute(SCHEMA_VERSION_ATTRIBUTE))
            builder.addPropertyValue("schemaVersion", element.getAttribute(SCHEMA_VERSION_ATTRIBUTE));

        // Get storage ID generator bean name (optional)
        final boolean autogenStorageIds = !element.hasAttribute(AUTO_GENERATE_STORAGE_IDS_ATTRIBUTE)
          || Boolean.valueOf(element.getAttribute(AUTO_GENERATE_STORAGE_IDS_ATTRIBUTE));
        if (!autogenStorageIds)
            builder.addPropertyValue("storageIdGenerator", null);
        if (element.hasAttribute(STORAGE_ID_GENERATOR_ATTRIBUTE)) {
            if (!autogenStorageIds) {
                parserContext.getReaderContext().fatal("<" + element.getTagName() + "> cannot have a `"
                  + STORAGE_ID_GENERATOR_ATTRIBUTE + "' attribute and " + AUTO_GENERATE_STORAGE_IDS_ATTRIBUTE + "=\"false\"",
                  parserContext.extractSource(element));
                return;
            }
            builder.addPropertyReference("storageIdGenerator", element.getAttribute(STORAGE_ID_GENERATOR_ATTRIBUTE));
        }

        // Construct PermazenFactoryBean bean definition
        builder.getRawBeanDefinition().setBeanClass(PermazenFactoryBean.class);

        // Look for nested <scan-classes> and <scan-field-types>
        final NodeList nodeList = element.getChildNodes();
        for (int i = 0; i < nodeList.getLength(); i++) {
            if (!(nodeList.item(i) instanceof Element))
                continue;
            final Element child = (Element)nodeList.item(i);
            if (!PermazenNamespaceHandler.JSIMPLEDB_NAMESPACE_URI.equals(child.getNamespaceURI()))
                continue;
            if (child.getLocalName().equals(PermazenNamespaceHandler.SCAN_CLASSES_TAG))
                builder.addPropertyValue("modelClasses", new ScanClassesBeanDefinitionParser().parse(child, parserContext));
            else if (child.getLocalName().equals(PermazenNamespaceHandler.SCAN_FIELD_TYPES_TAG)) {
                builder.addPropertyValue("fieldTypeClasses",
                  new ScanFieldTypesBeanDefinitionParser().parse(child, parserContext));
            } else {
                parserContext.getReaderContext().fatal("unsupported <" + child.getTagName() + "> element found"
                  + " inside <" + element.getTagName() + "> element", parserContext.extractSource(element));
                return;
            }
        }
    }
}

