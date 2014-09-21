
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.beans.BeanInfo;
import java.beans.IndexedPropertyDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;

import org.dellroad.stuff.java.Primitive;
import org.jsimpledb.JField;
import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleField;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.ParseUtil;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.SpaceParser;
import org.jsimpledb.parse.func.AbstractFunction;
import org.jsimpledb.parse.util.AddSuffixFunction;

public class BaseExprParser implements Parser<Node> {

    public static final BaseExprParser INSTANCE = new BaseExprParser();

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Node parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Parse initial atom
        Node node = new AtomParser(
          Iterables.transform(session.getFunctions().keySet(), new AddSuffixFunction("("))).parse(session, ctx, complete);

        // Handle new
        if (node instanceof IdentNode && ((IdentNode)node).getName().equals("new")) {

            // Get class name (or array type base), which must be a sequence of identifiers connected via dots
            new SpaceParser(true).parse(ctx, complete);
            final Matcher firstMatcher = ctx.tryPattern(IdentNode.NAME_PATTERN);
            if (firstMatcher == null)
                throw new ParseException(ctx, "expected class name");
            String className = firstMatcher.group();
            while (true) {
                final Matcher matcher = ctx.tryPattern("\\s*\\.\\s*(" + IdentNode.NAME_PATTERN + ")");
                if (matcher == null)
                    break;
                className += "." + matcher.group(1);
            }

            // Get array dimensions, if any
            final ArrayList<Node> dims = new ArrayList<>();
            boolean sawNull = false;
            while (true) {
                ctx.skipWhitespace();
                if (!ctx.tryLiteral("["))
                    break;
                ctx.skipWhitespace();
                if (ctx.tryLiteral("]")) {
                    dims.add(null);
                    sawNull = true;
                    continue;
                }
                if (!sawNull) {
                    dims.add(ExprParser.INSTANCE.parse(session, ctx, complete));
                    ctx.skipWhitespace();
                }
                if (!ctx.tryLiteral("]"))
                    throw new ParseException(ctx, "expected `]'").addCompletion("]");
            }

            // Resolve (base) class name
            final Class<?> baseClass;
            final Primitive<?> primitive;
            if (!dims.isEmpty() && (primitive = Primitive.forName(className)) != null) {
                if (primitive == Primitive.VOID)
                    throw new ParseException(ctx, "illegal instantiation of void array");
                baseClass = primitive.getType();
            } else {
                baseClass = session.resolveClass(className);
                if (baseClass == null)
                    throw new ParseException(ctx, "unknown class `" + className + "'");     // TODO: tab-completions
            }

            // Handle non-array
            if (dims.isEmpty()) {

                // Parse parameters
                if (!ctx.tryLiteral("("))
                    throw new ParseException(ctx, "expected `('").addCompletion("(");
                ctx.skipWhitespace();
                final List<Node> paramNodes = BaseExprParser.parseParams(session, ctx, complete);

                // Return constructor invocation node
                node = new Node() {
                    @Override
                    public Value evaluate(final ParseSession session) {
                        if (baseClass.isInterface())
                            throw new EvalException("invalid instantiation of " + baseClass);
                        final Object[] params = Lists.transform(paramNodes, new com.google.common.base.Function<Node, Object>() {
                            @Override
                            public Object apply(Node param) {
                                return param.evaluate(session).get(session);
                            }
                        }).toArray();
                        for (Constructor<?> constructor : baseClass.getConstructors()) {
                            final Class<?>[] ptypes = constructor.getParameterTypes();
                            if (ptypes.length != params.length)
                                continue;
                            try {
                                return new ConstValue(constructor.newInstance(params));
                            } catch (IllegalArgumentException e) {
                                continue;                               // wrong method, a parameter type didn't match
                            } catch (Exception e) {
                                final Throwable t = e instanceof InvocationTargetException ?
                                  ((InvocationTargetException)e).getTargetException() : e;
                                throw new EvalException("error invoking constructor `" + baseClass.getName() + "()': " + t, t);
                            }
                        }
                        throw new EvalException("no compatible constructor found in " + baseClass);
                    }
                };
            } else {                                        // handle array

                // Check number of dimensions
                if (dims.size() > 255)
                    throw new ParseException(ctx, "too many array dimensions (" + dims.size() + " > 255)");

                // Array literal must be present if and only if no dimensions are given
                final List<?> literal = dims.get(0) == null ?
                  this.parseArrayLiteral(session, ctx, complete, this.getArrayClass(baseClass, dims.size() - 1)) : null;

                // Return array instantiation invocation node
                node = new Node() {
                    @Override
                    public Value evaluate(final ParseSession session) {
                        final Class<?> elemType = BaseExprParser.this.getArrayClass(baseClass, dims.size() - 1);
                        return new ConstValue(literal != null ?
                          this.createLiteral(session, elemType, literal) : this.createEmpty(session, elemType, dims));
                    }

                    private Object createEmpty(ParseSession session, Class<?> elemType, List<Node> dims) {
                        final int length = dims.get(0).evaluate(session).checkIntegral(session, "array creation");
                        final Object array = Array.newInstance(elemType, length);
                        final List<Node> remainingDims = dims.subList(1, dims.size());
                        if (!remainingDims.isEmpty() && remainingDims.get(0) != null) {
                            for (int i = 0; i < length; i++)
                                Array.set(array, i, this.createEmpty(session, elemType.getComponentType(), remainingDims));
                        }
                        return array;
                    }

                    private Object createLiteral(ParseSession session, Class<?> elemType, List<?> values) {
                        final int length = values.size();
                        final Object array = Array.newInstance(elemType, length);
                        for (int i = 0; i < length; i++) {
                            if (elemType.isArray()) {
                                Array.set(array, i, this.createLiteral(session,
                                  elemType.getComponentType(), (List<?>)values.get(i)));
                            } else {
                                try {
                                    Array.set(array, i, ((Node)values.get(i)).evaluate(session).get(session));
                                } catch (Exception e) {
                                    throw new EvalException("error setting array value: " + e, e);
                                }
                            }
                        }
                        return array;
                    }
                };
            }
        }

        // Repeatedly parse operators (this gives left-to-right association)
        while (true) {

            // Parse operator, if any (one of `[', `.', `(', `++', or `--')
            final Matcher opMatcher = ctx.tryPattern("\\s*(\\[|\\.|\\(|\\+{2}|-{2})");
            if (opMatcher == null)
                return node;
            final String opsym = opMatcher.group(1);
            final int mark = ctx.getIndex();

            // Handle operators
            switch (opsym) {
            case "(":
            {
                // Atom must be an identifier, the name of a global function being invoked
                if (!(node instanceof IdentNode))
                    throw new ParseException(ctx);
                final String functionName = ((IdentNode)node).getName();
                final AbstractFunction function = session.getFunctions().get(functionName);
                if (function == null) {
                    throw new ParseException(ctx, "unknown function `" + functionName + "()'")
                      .addCompletions(ParseUtil.complete(session.getFunctions().keySet(), functionName));
                }

                // Parse function parameters
                ctx.skipWhitespace();
                final Object params = function.parseParams(session, ctx, complete);

                // Return node that applies the function to the parameters
                node = new Node() {
                    @Override
                    public Value evaluate(ParseSession session) {
                        return function.apply(session, params);
                    }
                };
                break;
            }
            case ".":
            {
                // Parse next atom - it must be an identifier, either a method or property name
                ctx.skipWhitespace();
                final Node memberNode = AtomParser.INSTANCE.parse(session, ctx, complete);
                if (!(memberNode instanceof IdentNode)) {
                    ctx.setIndex(mark);
                    throw new ParseException(ctx);
                }
                String member = ((IdentNode)memberNode).getName();

                // If first atom was an identifier, this must be a class name followed by a field or method name
                Class<?> cl = null;
                if (node instanceof IdentNode) {

                    // Keep parsing identifiers as long as we can; after each identifier, try to resolve a class name
                    final TreeMap<Integer, Class<?>> classes = new TreeMap<>();
                    String idents = ((IdentNode)node).getName();
                    while (true) {
                        classes.put(ctx.getIndex(), session.resolveClass(idents));
                        final Matcher matcher = ctx.tryPattern("\\s*\\.\\s*(" + IdentNode.NAME_PATTERN + ")");
                        if (matcher == null)
                            break;
                        idents += "." + member;
                        member = matcher.group(1);
                    }

                    // Use the longest class name resolved, if any
                    for (Map.Entry<Integer, Class<?>> entry : classes.descendingMap().entrySet()) {
                        if (entry.getValue() != null) {
                            cl = entry.getValue();
                            ctx.setIndex(entry.getKey());
                            break;
                        }
                    }
                    if (cl == null)
                        throw new ParseException(ctx, "unknown class `" + idents + "'");        // TODO: tab-completions
                }

                // Handle property access
                if (ctx.tryPattern("\\s*\\(") == null) {                                        // not a method call
                    final String propertyName = member;
                    final Node target = node;

                    // Handle "class" when preceded by a class name
                    if (cl != null && propertyName.equals("class")) {
                        node = new LiteralNode(cl);
                        break;
                    }

                    // Handle static fields
                    if (cl != null) {

                        // Get static field
                        final Field field;
                        try {
                            field = cl.getField(propertyName);
                        } catch (NoSuchFieldException e) {
                            throw new ParseException(ctx, "class `" + cl.getName() + "' has no field named `"
                              + propertyName + "'", e);
                        }
                        if ((field.getModifiers() & Modifier.STATIC) == 0) {
                            throw new ParseException(ctx, "field `" + propertyName + "' in class `" + cl.getName()
                              + "' is not a static field");
                        }

                        // Return node accessing it
                        node = new ConstNode(new StaticFieldValue(field));
                        break;
                    }

                    // Must be object property access
                    node = new Node() {
                        @Override
                        public Value evaluate(ParseSession session) {
                            return BaseExprParser.this.evaluateProperty(session, target.evaluate(session), propertyName);
                        }
                    };
                    break;
                }

                // Handle method call
                node = cl != null ?
                  new MethodInvokeNode(cl, member, BaseExprParser.parseParams(session, ctx, complete)) :
                  new MethodInvokeNode(node, member, BaseExprParser.parseParams(session, ctx, complete));
                break;
            }
            case "[":
            {
                this.spaceParser.parse(ctx, complete);
                final Node index = AssignmentExprParser.INSTANCE.parse(session, ctx, complete);
                this.spaceParser.parse(ctx, complete);
                if (!ctx.tryLiteral("]"))
                    throw new ParseException(ctx).addCompletion("] ");
                final Node target = node;
                node = new Node() {
                    @Override
                    public Value evaluate(ParseSession session) {
                        return Op.ARRAY_ACCESS.apply(session, target.evaluate(session), index.evaluate(session));
                    }
                };
                break;
            }
            case "++":
                node = this.createPostcrementNode("increment", node, true);
                break;
            case "--":
                node = this.createPostcrementNode("decrement", node, false);
                break;
            default:
                throw new RuntimeException("internal error: " + opsym);
            }
        }
    }

    // Parse array literal
    private List<?> parseArrayLiteral(ParseSession session, ParseContext ctx, boolean complete, Class<?> elemType) {
        final ArrayList<Object> list = new ArrayList<>();
        this.spaceParser.parse(ctx, complete);
        if (!ctx.tryLiteral("{"))
            throw new ParseException(ctx).addCompletion("{ ");
        this.spaceParser.parse(ctx, complete);
        if (ctx.tryLiteral("}"))
            return list;
        while (true) {
            list.add(elemType.isArray() ?
              this.parseArrayLiteral(session, ctx, complete, elemType.getComponentType()) :
              ExprParser.INSTANCE.parse(session, ctx, complete));
            ctx.skipWhitespace();
            if (ctx.tryLiteral("}"))
                break;
            if (!ctx.tryLiteral(","))
                throw new ParseException(ctx, "expected `,'").addCompletion(", ");
            this.spaceParser.parse(ctx, complete);
        }
        return list;
    }

    // Get the array class with the given base type and dimensions
    private Class<?> getArrayClass(Class<?> base, int dimensions) {
        if (dimensions == 0)
            return base;
        final String suffix = base.isArray() ? base.getName() :
          base.isPrimitive() ? "" + Primitive.get(base).getLetter() : "L" + base.getName() + ";";
        final StringBuilder buf = new StringBuilder(dimensions + suffix.length());
        while (dimensions-- > 0)
            buf.append('[');
        buf.append(suffix);
        final String className = buf.toString();
        try {
            return Class.forName(className, false, Thread.currentThread().getContextClassLoader());
        } catch (Exception e) {
            throw new EvalException("can't load array class `" + className + "'");
        }
    }

    // Parse method parameters; we assume opening `(' has just been parsed
    static List<Node> parseParams(ParseSession session, ParseContext ctx, boolean complete) {
        final ArrayList<Node> params = new ArrayList<Node>();
        final SpaceParser spaceParser = new SpaceParser();
        spaceParser.parse(ctx, complete);
        if (ctx.tryLiteral(")"))
            return params;
        while (true) {
            params.add(ExprParser.INSTANCE.parse(session, ctx, complete));
            spaceParser.parse(ctx, complete);
            if (ctx.tryLiteral(")"))
                break;
            if (!ctx.tryLiteral(","))
                throw new ParseException(ctx, "expected `,'").addCompletion(", ");
            spaceParser.parse(ctx, complete);
        }
        return params;
    }

    private Value evaluateProperty(ParseSession session, Value value, final String name) {

        // Evaluate target
        final Object target = value.checkNotNull(session, "property `" + name + "' access");
        final Class<?> cl = target.getClass();

        // Handle properties of database objects (i.e., database fields)
        if (session.hasJSimpleDB() && target instanceof JObject) {

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
        } else if (!session.hasJSimpleDB() && target instanceof ObjId) {
            final ObjId id = (ObjId)target;

            // Resolve field
            org.jsimpledb.core.Field<?> field0;
            try {
                field0 = ParseUtil.resolveField(session, id, name);
            } catch (IllegalArgumentException e) {
                field0 = null;
            }
            final org.jsimpledb.core.Field<?> field = field0;

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
            if (propertyDescriptor.getReadMethod() != null && propertyDescriptor.getWriteMethod() != null)
                return new MutableBeanPropertyValue(target, propertyDescriptor);
            else if (propertyDescriptor.getReadMethod() != null)
                return new BeanPropertyValue(target, propertyDescriptor);
        }

        // Try instance field
        /*final*/ Field javaField;
        try {
            javaField = cl.getField(name);
        } catch (NoSuchFieldException e) {
            javaField = null;
        }
        if (javaField != null)
            return new ObjectFieldValue(target, javaField);

        // Try array.length
        if (target.getClass().isArray() && name.equals("length"))
            return new ConstValue(Array.getLength(target));

        // Not found
        throw new EvalException("property `" + name + "' not found in " + cl);
    }

    private Node createMethodInvokeNode(final Object target, final String name, final List<Node> paramNodes) {
        return new Node() {
            @Override
            public Value evaluate(final ParseSession session) {

                // Evaluate params
                final Object[] params = Lists.transform(paramNodes, new com.google.common.base.Function<Node, Object>() {
                    @Override
                    public Object apply(Node param) {
                        return param.evaluate(session).get(session);
                    }
                }).toArray();

                // Handle static method
                if (!(target instanceof Node))
                    return this.invokeMethod((Class<?>)target, null, name, params);

                // Handle instance method
                final Object obj = ((Node)target).evaluate(session).checkNotNull(session, "method " + name + "() invocation");
                return this.invokeMethod(obj.getClass(), obj, name, params);
            }

            private Value invokeMethod(Class<?> cl, Object obj, String name, Object[] params) {

                // Try interface methods
                for (Class<?> iface : this.addInterfaces(cl, new LinkedHashSet<Class<?>>())) {
                    for (Method method : iface.getMethods()) {
                        final Value value = this.tryMethod(method, obj, name, params);
                        if (value != null)
                            return value;
                    }
                }

                // Try class methods
                for (Method method : cl.getMethods()) {
                    final Value value = this.tryMethod(method, obj, name, params);
                    if (value != null)
                        return value;
                }

                // Not found
                throw new EvalException("no compatible method `" + name + "()' found in " + cl);
            }

            private Set<Class<?>> addInterfaces(Class<?> cl, Set<Class<?>> interfaces) {
                for (Class<?> iface : cl.getInterfaces()) {
                    interfaces.add(iface);
                    this.addInterfaces(iface, interfaces);
                }
                if (cl.getSuperclass() != null)
                    this.addInterfaces(cl.getSuperclass(), interfaces);
                return interfaces;
            }

            private Value tryMethod(Method method, Object obj, String name, Object[] params) {
                if (!method.getName().equals(name))
                    return null;
                final Class<?>[] ptypes = method.getParameterTypes();
                if (method.isVarArgs()) {
                    if (params.length < ptypes.length - 1)
                        return null;
                    Object[] newParams = new Object[ptypes.length];
                    System.arraycopy(params, 0, newParams, 0, ptypes.length - 1);
                    Object[] varargs = new Object[params.length - (ptypes.length - 1)];
                    System.arraycopy(params, ptypes.length - 1, varargs, 0, varargs.length);
                    newParams[ptypes.length - 1] = varargs;
                    params = newParams;
                } else if (params.length != ptypes.length)
                    return null;
                final Object result;
                try {
                    result = method.invoke(obj, params);
                } catch (IllegalArgumentException e) {
                    return null;                            // a parameter type didn't match -> wrong method
                } catch (Exception e) {
                    final Throwable t = e instanceof InvocationTargetException ?
                      ((InvocationTargetException)e).getTargetException() : e;
                    throw new EvalException("error invoking method `" + name + "()' on "
                      + (obj != null ? "object of type " + obj.getClass().getName() : method.getDeclaringClass()) + ": " + t, t);
                }
                return result != null || method.getReturnType() != Void.TYPE ? new ConstValue(result) : Value.NO_VALUE;
            }
        };
    }

    private Node createPostcrementNode(final String operation, final Node node, final boolean increment) {
        return new Node() {
            @Override
            public Value evaluate(ParseSession session) {
                return node.evaluate(session).xxcrement(session, "post-" + operation, increment);
            }
        };
    }
}

