
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.expr;

import io.permazen.JField;
import io.permazen.JObject;
import io.permazen.JSimpleField;
import io.permazen.core.ObjId;
import io.permazen.core.SimpleField;
import io.permazen.parse.ParseException;
import io.permazen.parse.ParseSession;
import io.permazen.parse.ParseUtil;
import io.permazen.parse.Parser;
import io.permazen.parse.SpaceParser;
import io.permazen.util.ParseContext;

import java.beans.BeanInfo;
import java.beans.IndexedPropertyDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.regex.Matcher;

/**
 * Parses basic left-associative Java expressions such as auto-increment expressions, array access, field access, invocation, etc.
 *
 * <p>
 * Includes these extensions:
 * <ul>
 *  <li>Access to Java bean methods via "property syntax", e.g., {@code foo.name = "fred"} means {@code foo.setName("fred")}</li>
 *  <li>Access database object fields via "property syntax" given an object ID, e.g., <code>@fc21bf6d8930a215.name</code></li>
 *  <li>Array syntax for {@link java.util.Map} and {@link java.util.List} value access, e.g., {@code mymap[key] = value} means
 *      {@code mymap.put(key, value)}, and {@code mylist[12] = "abc"} means {@code mylist.set(12, "abc")}</li>
 * </ul>
 */
public class BaseExprParser implements Parser<Node> {

    public static final BaseExprParser INSTANCE = new BaseExprParser();

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Node parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Parse initial atom
        Node node = AtomExprParser.INSTANCE.parse(session, ctx, complete);

        // Repeatedly parse post-operators, giving left-to-right association
        // These are: foo[x], foo::x, foo.x(), foo.x, foo++, foo--
        while (true) {

            // Skip whitespace
            ctx.skipWhitespace();

            // Handle array reference
            if (ctx.tryLiteral("[")) {
                this.spaceParser.parse(ctx, complete);
                final Node index = ExprParser.INSTANCE.parse(session, ctx, complete);
                this.spaceParser.parse(ctx, complete);
                if (!ctx.tryLiteral("]"))
                    throw new ParseException(ctx).addCompletion("]");
                final Node target = node;
                node = new Node() {
                    @Override
                    public Value evaluate(ParseSession session) {
                        return Op.ARRAY_ACCESS.apply(session, target.evaluate(session), index.evaluate(session));
                    }

                    @Override
                    public Class<?> getType(ParseSession session) {
                        final Class<?> arrayType = target.getType(session);
                        return arrayType.isArray() ? arrayType.getComponentType() : Object.class;
                    }
                };
                continue;
            }

            // Handle bound method reference
            if (ctx.tryLiteral("::")) {
                ctx.skipWhitespace();
                final Matcher methodMatch = ctx.tryPattern(ParseUtil.IDENT_PATTERN);
                if (methodMatch == null)
                    throw new ParseException(ctx, "expected method name");
                node = new BoundMethodReferenceNode(node, methodMatch.group());
                continue;
            }

            // Support tab-completion for "<expr>.foo", based on the type of <expr>
            if (complete) {
                final Matcher propertyPrefixMatch = ctx.tryPattern("\\.\\s*(" + ParseUtil.IDENT_PATTERN + ")?$");
                if (propertyPrefixMatch != null) {
                    String prefix = propertyPrefixMatch.group(1);
                    if (prefix == null)
                        prefix = "";
                    throw new ParseException(ctx)
                      .addCompletions(AtomExprParser.getInstanceMemberCompletions(node.getType(session), prefix));
                }
            }

            // Handle instance method invocation
            final Matcher invokeMatch = ctx.tryPattern("\\.\\s*(" + ParseUtil.IDENT_PATTERN + ")\\s*\\(");
            if (invokeMatch != null) {
                node = new MethodInvokeNode(node, invokeMatch.group(1), AtomExprParser.parseParams(session, ctx, complete));
                continue;
            }

            // Handle property/field reference
            final Matcher propertyMatch = ctx.tryPattern("\\.\\s*(" + ParseUtil.IDENT_PATTERN + ")\\s*");
            if (propertyMatch != null) {
                final String propertyName = propertyMatch.group(1);
                final Node target = node;
                node = new Node() {
                    @Override
                    public Value evaluate(ParseSession session) {
                        return BaseExprParser.this.evaluateProperty(session, target.evaluate(session), propertyName);
                    }

                    @Override
                    public Class<?> getType(ParseSession session) {
                        return BaseExprParser.this.getPropertyType(session, target, propertyName);
                    }
                };
                continue;
            }

            // Handle post-increment
            if (ctx.tryLiteral("++")) {
                node = this.createPostcrementNode("increment", node, true);
                continue;
            }

            // Handle post-decrement
            if (ctx.tryLiteral("--")) {
                node = this.createPostcrementNode("decrement", node, false);
                continue;
            }

            // We're done
            break;
        }

        // Done
        return node;
    }

    private Value evaluateProperty(ParseSession session, Value value, final String name) {

        // Evaluate target
        final Object target = value.checkNotNull(session, "property `" + name + "' access");
        final Class<?> cl = target.getClass();

        // Handle properties of database objects (i.e., database fields)
        if (session.getMode().hasJSimpleDB() && target instanceof JObject) {

            // Get object and ID
            final JObject jobj = (JObject)target;
            final ObjId id = jobj.getObjId();

            // Resolve JField
            JField jfield0;
            try {
                jfield0 = ParseUtil.resolveJField(session, id, name);
            } catch (IllegalArgumentException e) {
                jfield0 = null;
            }
            final JField jfield = jfield0;

            // Return value reflecting the field if the field was found
            if (jfield instanceof JSimpleField)
                return new JSimpleFieldValue(jobj, (JSimpleField)jfield);
            else if (jfield != null)
                return new JFieldValue(jobj, jfield);
        } else if (session.getMode().hasCoreAPI() && target instanceof ObjId) {
            final ObjId id = (ObjId)target;

            // Resolve field
            io.permazen.core.Field<?> field0;
            try {
                field0 = ParseUtil.resolveField(session, id, name);
            } catch (IllegalArgumentException e) {
                field0 = null;
            }
            final io.permazen.core.Field<?> field = field0;

            // Return value reflecting the field if the field was found
            if (field instanceof SimpleField)
                return new SimpleFieldValue(id, (SimpleField<?>)field);
            else if (field != null)
                return new FieldValue(id, field);
        }

        // Try bean property accessed via bean methods
        final BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo(cl);
        } catch (IntrospectionException e) {
            throw new EvalException("error introspecting class `" + cl.getName() + "': " + e, e);
        }
        for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {
            if (propertyDescriptor instanceof IndexedPropertyDescriptor)
                continue;
            if (!propertyDescriptor.getName().equals(name))
                continue;
            final Method getter = propertyDescriptor.getReadMethod() != null ?
              MethodUtil.makeAccessible(propertyDescriptor.getReadMethod()) : null;
            final Method setter = propertyDescriptor.getWriteMethod() != null ?
              MethodUtil.makeAccessible(propertyDescriptor.getWriteMethod()) : null;
            if (getter != null && setter != null)
                return new MutableBeanPropertyValue(target, propertyDescriptor.getName(), getter, setter);
            else if (getter != null)
                return new BeanPropertyValue(target, propertyDescriptor.getName(), getter);
        }

        // Try instance field
        /*final*/ Field javaField;
        try {
            javaField = AtomExprParser.findField(cl, name, false);
        } catch (NoSuchFieldException e) {
            javaField = null;
        }
        if (javaField != null)
            return new InstanceFieldValue(target, javaField);

        // Try array.length
        if (target.getClass().isArray() && name.equals("length"))
            return new ConstValue(Array.getLength(target));

        // Not found
        throw new EvalException("property `" + name + "' not found in " + cl);
    }

    private Class<?> getPropertyType(ParseSession session, Node node, final String name) {

        // Try bean property accessed via bean methods
        final Class<?> cl = node.getType(session);
        final BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo(cl);
        } catch (IntrospectionException e) {
            return Object.class;
        }
        for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {
            if (propertyDescriptor instanceof IndexedPropertyDescriptor)
                continue;
            if (!propertyDescriptor.getName().equals(name))
                continue;
            if (propertyDescriptor.getReadMethod() == null)
                continue;
            return propertyDescriptor.getReadMethod().getReturnType();
        }

        // Try instance field
        /*final*/ Field javaField;
        try {
            javaField = AtomExprParser.findField(cl, name, false);
        } catch (NoSuchFieldException e) {
            javaField = null;
        }
        if (javaField != null)
            return javaField.getType();

        // Try array.length
        if (cl.isArray() && name.equals("length"))
            return Integer.class;

        // Dunno
        return Object.class;
    }

    private Node createPostcrementNode(final String operation, final Node node, final boolean increment) {
        return new Node() {
            @Override
            public Value evaluate(ParseSession session) {
                return node.evaluate(session).xxcrement(session, "post-" + operation, increment);
            }

            @Override
            public Class<?> getType(ParseSession session) {
                return node.getType(session);
            }
        };
    }
}

