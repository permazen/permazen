
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.spring;

import org.dellroad.stuff.schema.SQLDatabaseAction;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.Text;

/**
 * Parses <code>&lt;dellroad-stuff:sql&gt;</code> tags.
 */
class SQLBeanDefinitionParser extends AbstractSingleBeanDefinitionParser {

    private static final String SPLIT_PATTERN_ATTRIBUTE = "split-pattern";
    private static final String RESOURCE_ATTRIBUTE = "resource";
    private static final String CHARSET_ATTRIBUTE = "charset";

    private boolean ignoreId;

    public SQLBeanDefinitionParser() {
    }

    SQLBeanDefinitionParser(boolean ignoreId) {
        this.ignoreId = ignoreId;
    }

    @Override
    protected boolean shouldGenerateId() {
        return this.ignoreId ? true : super.shouldGenerateId();
    }

    @Override
    protected Class<SQLDatabaseAction> getBeanClass(Element element) {
        return SQLDatabaseAction.class;
    }

    @Override
    protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

        // Get "split-pattern" attribute
        Attr attr = element.getAttributeNodeNS(null, SPLIT_PATTERN_ATTRIBUTE);
        if (attr != null)
            builder.addPropertyValue("splitPattern", attr.getValue());

        // Get "resource" attribute or nested SQL
        attr = element.getAttributeNodeNS(null, RESOURCE_ATTRIBUTE);
        if (attr != null) {

            // Verify no nested content
            Node node = element.getFirstChild();
            if (node != null)
                this.bogus(element, parserContext);

            // Create resource reader
            BeanDefinitionBuilder resourceBuilder = BeanDefinitionBuilder.genericBeanDefinition(ResourceReaderFactoryBean.class);
            resourceBuilder.addPropertyValue("resource", attr.getValue());

            // Apply "charset" if any
            attr = element.getAttributeNodeNS(null, CHARSET_ATTRIBUTE);
            if (attr != null)
                resourceBuilder.addPropertyValue("characterEncoding", attr.getValue());

            // Set resource reader as the factory bean for the SQL script
            builder.addPropertyValue("SQLScript", resourceBuilder.getBeanDefinition());
        } else {

            // Configure SQL script
            builder.addPropertyValue("SQLScript", this.getNestedSQL(element, parserContext));
        }
    }

    String getNestedSQL(Element element, ParserContext parserContext) {

        // Verify at least one child node exists
        Node node = element.getFirstChild();
        if (node == null)
            this.bogus(element, parserContext);

        // Concatenate all child nodes, which must be text
        StringBuilder buf = new StringBuilder();
        while (node != null) {
            if (!(node instanceof Text))
                this.bogus(element, parserContext);
            buf.append(((Text)node).getData());
            node = node.getNextSibling();
        }

        // Done
        return buf.toString();
    }

    private void bogus(Element element, ParserContext parserContext) {
        String message = "<" + element.getTagName() + "> beans must have either"
          + " a \"" + RESOURCE_ATTRIBUTE + "\" attribute or nested SQL content, but not both";
        parserContext.getReaderContext().fatal(message, parserContext.extractSource(element));
    }
}

