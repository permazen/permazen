
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

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
import org.jsimpledb.JTransaction;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.func.Function;
import org.jsimpledb.cli.parse.ParseException;
import org.jsimpledb.cli.parse.ParseUtil;
import org.jsimpledb.cli.parse.Parser;
import org.jsimpledb.cli.parse.SpaceParser;
import org.jsimpledb.cli.util.AddSuffixFunction;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.util.ParseContext;

public class BaseExprParser implements Parser<Node> {

    public static final BaseExprParser INSTANCE = new BaseExprParser();

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Node parse(Session session, ParseContext ctx, boolean complete) {

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
                    public Value evaluate(final Session session) {
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
                                return new Value(constructor.newInstance(params));
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
                    public Value evaluate(final Session session) {
                        final Class<?> elemType = BaseExprParser.this.getArrayClass(baseClass, dims.size() - 1);
                        return new Value(literal != null ?
                          this.createLiteral(session, elemType, literal) : this.createEmpty(session, elemType, dims));
                    }

                    private Object createEmpty(Session session, Class<?> elemType, List<Node> dims) {
                        final int length = dims.get(0).evaluate(session).checkIntegral(session, "array creation");
                        final Object array = Array.newInstance(elemType, length);
                        final List<Node> remainingDims = dims.subList(1, dims.size());
                        if (!remainingDims.isEmpty() && remainingDims.get(0) != null) {
                            for (int i = 0; i < length; i++)
                                Array.set(array, i, this.createEmpty(session, elemType.getComponentType(), remainingDims));
                        }
                        return array;
                    }

                    private Object createLiteral(Session session, Class<?> elemType, List<?> values) {
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
                final Function function = session.getFunctions().get(functionName);
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
                    public Value evaluate(Session session) {
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
                if (ctx.tryPattern("\\s*\\(") == null) {
                    final String propertyName = member;
                    final Node target = node;
                    final Class<?> cl2 = cl;
                    node = new Node() {
                        @Override
                        public Value evaluate(Session session) {
                            if (cl2 != null) {
                                return new Value(propertyName.equals("class") ?
                                  cl2 : BaseExprParser.this.readStaticField(cl2, propertyName));
                            }
                            return BaseExprParser.this.evaluateProperty(session, target.evaluate(session), propertyName);
                        }
                    };
                    break;
                }

                // Handle method call
                node = this.createMethodInvokeNode(cl != null ? cl : node,
                  member, BaseExprParser.parseParams(session, ctx, complete));
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
                    public Value evaluate(Session session) {
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
    private List<?> parseArrayLiteral(Session session, ParseContext ctx, boolean complete, Class<?> elemType) {
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
    static List<Node> parseParams(Session session, ParseContext ctx, boolean complete) {
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

    private Object readStaticField(Class<?> cl, String name) {
        try {
            return cl.getField(name).get(null);
        } catch (NoSuchFieldException e) {
            throw new EvalException("no field `" + name + "' found in class `" + cl.getName() + "'", e);
        } catch (NullPointerException e) {
            throw new EvalException("field `" + name + "' in class `" + cl.getName() + "' is not a static field");
        } catch (Exception e) {
            throw new EvalException("error reading field `" + name + "' in class `" + cl.getName() + "': " + e, e);
        }
    }

    private Value evaluateProperty(Session session, Value value, final String name) {

        // Evaluate target
        final Object target = value.checkNotNull(session, "property `" + name + "' access");
        final Class<?> cl = target.getClass();

        // Handle properties of database objects (i.e., database fields)
        if (session.hasJSimpleDB() && target instanceof JObject) {
            final ObjId id = ((JObject)target).getObjId();

            // Resolve JField
            JField jfield0;
            try {
                jfield0 = ParseUtil.resolveJField(session, id, name);
            } catch (IllegalArgumentException e) {
                jfield0 = null;
            }
            final JField jfield = jfield0;

            // Return value if found
            if (jfield != null) {
                return new Value(null, jfield instanceof JSimpleField ? new Setter() {
                    @Override
                    public void set(Session session, Value value) {
                        final Object obj = value.get(session);
                        try {
                            ((JSimpleField)jfield).setValue(JTransaction.getCurrent(), id, obj);
                        } catch (IllegalArgumentException e) {
                            throw new EvalException("invalid value of type " + (obj != null ? obj.getClass().getName() : "null")
                              + " for field `" + jfield.getName() + "'", e);
                        }
                    }
                } : null) {
                    @Override
                    public Object get(Session session) {
                        try {
                            return jfield.getValue(JTransaction.getCurrent(), id);
                        } catch (Exception e) {
                            throw new EvalException("error reading field `" + name + "' from object " + id + ": "
                              + (e.getMessage() != null ? e.getMessage() : e));
                        }
                    }
                };
            }
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

            // Return value if found
            if (field != null) {
                return new Value(field instanceof SimpleField ? new Setter() {
                    @Override
                    public void set(Session session, Value value) {
                        final Object obj = value.get(session);
                        try {
                            session.getTransaction().writeSimpleField(id, field.getStorageId(), obj, false);
                        } catch (IllegalArgumentException e) {
                            throw new EvalException("invalid value of type " + (obj != null ? obj.getClass().getName() : "null")
                              + " for " + field, e);
                        }
                    }
                } : null) {
                    @Override
                    public Object get(Session session) {
                        try {
                            return field.getValue(session.getTransaction(), id);
                        } catch (Exception e) {
                            throw new EvalException("error reading field `" + name + "' from object " + id + ": "
                              + (e.getMessage() != null ? e.getMessage() : e));
                        }
                    }
                };
            }
        }

        // Try bean property accessed via bean methods
        final BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo(cl);
        } catch (IntrospectionException e) {
            throw new EvalException("error introspecting class `" + cl.getName() + "': " + e, e);
        }
        for (PropertyDescriptor pd : beanInfo.getPropertyDescriptors()) {
            if (pd instanceof IndexedPropertyDescriptor)
                continue;
            if (!pd.getName().equals(name))
                continue;
            final Method readMethod = pd.getReadMethod();
            final Method writeMethod = pd.getWriteMethod();
            return new Value(null, writeMethod != null ? new Setter() {
                @Override
                public void set(Session session, Value value) {
                    final Object obj = value.get(session);
                    try {
                        writeMethod.invoke(target, obj);
                    } catch (Exception e) {
                        final Throwable t = e instanceof InvocationTargetException ?
                          ((InvocationTargetException)e).getTargetException() : e;
                        throw new EvalException("error writing property `" + name + "' to object of type "
                          + cl.getName() + ": " + t, t);
                    }
                }
            } : null) {
                @Override
                public Object get(Session session) {
                    try {
                        return readMethod.invoke(target);
                    } catch (Exception e) {
                        final Throwable t = e instanceof InvocationTargetException ?
                          ((InvocationTargetException)e).getTargetException() : e;
                        throw new EvalException("error reading property `" + name + "' from object of type "
                          + cl.getName() + ": " + t, t);
                    }
                }
            };
        }

        // Try instance field
        /*final*/ Field javaField;
        try {
            javaField = cl.getField(name);
        } catch (NoSuchFieldException e) {
            javaField = null;
        }
        if (javaField != null) {
            final Field javaField2 = javaField;
            return new DynamicValue() {
                @Override
                public void set(Session session, Value value) {
                    final Object obj = value.get(session);
                    try {
                        javaField2.set(target, obj);
                    } catch (Exception e) {
                        throw new EvalException("error setting field `" + name + "' in object of type "
                          + cl.getName() + ": " + e, e);
                    }
                }
                @Override
                public Object get(Session session) {
                    try {
                        return javaField2.get(target);
                    } catch (Exception e) {
                        throw new EvalException("error reading field `" + name + "' in object of type "
                          + cl.getName() + ": " + e, e);
                    }
                }
            };
        }

        // Handle array.length
        if (target.getClass().isArray() && name.equals("length"))
            return new Value(Array.getLength(target));

        // Not found
        throw new EvalException("property `" + name + "' not found in " + cl);
    }

    private Node createMethodInvokeNode(final Object target, final String name, final List<Node> paramNodes) {
        return new Node() {
            @Override
            public Value evaluate(final Session session) {

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
                return result != null || method.getReturnType() != Void.TYPE ? new Value(result) : Value.NO_VALUE;
            }
        };
    }

    private Node createPostcrementNode(final String operation, final Node node, final boolean increment) {
        return new Node() {
            @Override
            public Value evaluate(Session session) {
                final Value oldValue = node.evaluate(session);
                oldValue.xxcrement(session, "post-" + operation, increment);
                return oldValue;
            }
        };
    }
}

