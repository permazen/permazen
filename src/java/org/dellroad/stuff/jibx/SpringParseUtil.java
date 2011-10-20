
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.jibx;


import org.jibx.runtime.JiBXParseException;
import org.springframework.expression.Expression;
import org.springframework.expression.ParseException;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;

/**
 * JiBX parsing utility methods for Spring expressions.
 */
public final class SpringParseUtil {

    private SpringParseUtil() {
    }

    /**
     * Deserialize a Spring {@link Expression}.
     *
     * @see #serializeExpression
     */
    public static Expression deserializeExpression(String string) throws JiBXParseException {
        return SpringParseUtil.deserializeExpression(string, null);
    }

    /**
     * Deserialize a Spring {@link Expression} as a template expression. Equivalent to
     *  <blockquote><code>
     *  deserializeExpression(string, ParserContext.TEMPLATE_EXPRESSION);
     *  <code><blockquote>
     *
     * @see #serializeExpression
     * @see #deserializeExpression(String, ParserContext)
     */
    public static Expression deserializeTemplateExpression(String string) throws JiBXParseException {
        return SpringParseUtil.deserializeExpression(string, ParserContext.TEMPLATE_EXPRESSION);
    }

    /**
     * Deserialize a Spring {@link Expression} using the supplied {@link ParserContext}.
     *
     * @see #serializeExpression
     */
    public static Expression deserializeExpression(String string, ParserContext context) throws JiBXParseException {
        SpelExpressionParser parser = new SpelExpressionParser();
        try {
            return parser.parseExpression(string, context);
        } catch (ParseException e) {
            throw new JiBXParseException("invalid Spring EL expression", string, e);
        }
    }

    /**
     * Serialize a Spring {@link Expression}.
     *
     * @see #deserializeExpression
     */
    public static String serializeExpression(Expression expr) {
        return expr.getExpressionString();
    }
}

