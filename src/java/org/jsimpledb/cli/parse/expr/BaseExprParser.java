
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

import com.google.common.collect.Lists;

import java.beans.BeanInfo;
import java.beans.IndexedPropertyDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

import org.jsimpledb.JTransaction;
import org.jsimpledb.cli.Session;
import org.jsimpledb.cli.func.Function;
import org.jsimpledb.cli.parse.ParseException;
import org.jsimpledb.cli.parse.ParseUtil;
import org.jsimpledb.cli.parse.Parser;
import org.jsimpledb.cli.parse.SpaceParser;
import org.jsimpledb.core.CounterField;
import org.jsimpledb.core.FieldSwitchAdapter;
import org.jsimpledb.core.MapField;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.SetField;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.util.ParseContext;

public class BaseExprParser implements Parser<Node> {

    public static final BaseExprParser INSTANCE = new BaseExprParser();

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Node parse(Session session, ParseContext ctx, boolean complete) {

        // Parse initial atom
        Node node = AtomParser.INSTANCE.parse(session, ctx, complete);

        // Parse operators (this gives left-to-right association)
        while (true) {

            // Parse operator, if any
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
                if (function != null) {
                    final Object params = function.parseParams(session, ctx, complete);
                    node = new Node() {
                        @Override
                        public Value evaluate(Session session) {
                            return function.apply(session, params);
                        }
                    };
                    break;
                }
                throw new ParseException(ctx, "unknown function `" + functionName + "()'")
                  .addCompletions(ParseUtil.complete(session.getFunctions().keySet(), functionName));
            }
            case ".":
            {
                // Parse next atom - it must be an identifier, either a method or property name
                this.spaceParser.parse(ctx, complete);
                final Node memberNode = AtomParser.INSTANCE.parse(session, ctx, complete);
                if (!(memberNode instanceof IdentNode)) {
                    ctx.setIndex(mark);
                    throw new ParseException(ctx);
                }
                String member = ((IdentNode)memberNode).getName();

                // If first atom was an identifier, this must be a class name followed by a field or method name
                Class<?> cl = null;
                if (node instanceof IdentNode) {

                    // Keep parsing identifiers until we recognize a class name
                    String className = ((IdentNode)node).getName();
                    while (true) {
                        if ((cl = session.resolveClass(className)) != null)
                            break;
                        final Matcher matcher = ctx.tryPattern("\\s*\\.\\s*(" + IdentNode.NAME_PATTERN + ")");
                        if (matcher == null)
                            throw new ParseException(ctx, "unknown class `" + className + "'");     // TODO: tab-completions
                        className += "." + member;
                        member = matcher.group(1);
                    }
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
                throw new ParseException(ctx).addCompletion(", ");
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

        // Evaluate value, converting ObjId into JObject (if possible)
        Object obj0 = value.checkNotNull(session, "property `" + name + "' access");
        if (obj0 instanceof ObjId && session.getJSimpleDB() != null)
            obj0 = JTransaction.getCurrent().getJObject((ObjId)obj0);
        final Object obj = obj0;

        // Get object class
        final Class<?> cl = obj.getClass();

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
                public void set(Session session, Object value) {
                    try {
                        writeMethod.invoke(obj, value);
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
                        return readMethod.invoke(obj);
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
                public void set(Session session, Object value) {
                    try {
                        javaField2.set(obj, value);
                    } catch (Exception e) {
                        throw new EvalException("error setting field `" + name + "' in object of type "
                          + cl.getName() + ": " + e, e);
                    }
                }
                @Override
                public Object get(Session session) {
                    try {
                        return javaField2.get(obj);
                    } catch (Exception e) {
                        throw new EvalException("error reading field `" + name + "' in object of type "
                          + cl.getName() + ": " + e, e);
                    }
                }
            };
        }

        // Handle properties of database objects (i.e., database fields)
        if (obj instanceof ObjId) {
            final ObjId id = (ObjId)obj;
            final Transaction tx = session.getTransaction();
            final org.jsimpledb.core.Field<?> field;
            try {
                field = ParseUtil.resolveField(session, id, name);
            } catch (IllegalArgumentException e) {
                throw new EvalException(e.getMessage());
            }
            return field.visit(new FieldSwitchAdapter<Value>() {

                @Override
                public <E> Value caseSetField(SetField<E> field) {
                    return new Value(tx.readSetField(id, field.getStorageId(), false));
                }

                @Override
                public <K, V> Value caseMapField(MapField<K, V> field) {
                    return new Value(tx.readMapField(id, field.getStorageId(), false));
                }

                @Override
                public <T> Value caseSimpleField(final SimpleField<T> field) {
                    return new Value(tx.readSimpleField(id, field.getStorageId(), false), new Setter() {
                        @Override
                        public void set(Session session, Object value) {
                            try {
                                session.getTransaction().writeSimpleField(id, field.getStorageId(), value, false);
                            } catch (IllegalArgumentException e) {
                                throw new EvalException("invalid value of type " + value.getClass().getName() + " for " + field, e);
                            }
                        }
                    });
                }

                @Override
                public Value caseCounterField(CounterField field) {
                    return new Value(tx.readCounterField(id, field.getStorageId(), false));
                }
            });
        }

        // Not found
        throw new EvalException("property `" + name + "' not found in " + cl);
    }

    private Node createMethodInvokeNode(final Object target, final String name, final List<Node> paramNodes) {
        return new Node() {
            @Override
            public Value evaluate(final Session session) {
                final Class<?> cl;
                final Object obj;
                if (target instanceof Node) {       // instance method
                    obj = ((Node)target).evaluate(session).checkNotNull(session, "method " + name + "() invocation");
                    cl = obj.getClass();
                } else {                            // static method
                    obj = null;
                    cl = (Class<?>)target;
                }
                final Object[] params = Lists.transform(paramNodes, new com.google.common.base.Function<Node, Object>() {
                    @Override
                    public Object apply(Node param) {
                        return param.evaluate(session).get(session);
                    }
                }).toArray();

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
                try {
                    return new Value(method.invoke(obj, params));
                } catch (IllegalArgumentException e) {
                    return null;                            // a parameter type didn't match -> wrong method
                } catch (Exception e) {
                    final Throwable t = e instanceof InvocationTargetException ?
                      ((InvocationTargetException)e).getTargetException() : e;
                    throw new EvalException("error invoking method `" + name + "()' on "
                      + (obj != null ? "object of type " + obj.getClass().getName() : method.getDeclaringClass()) + ": " + t, t);
                }
            }
        };
    }

    private Node createPostcrementNode(final String operation, final Node node, final boolean increment) {
        return new Node() {
            @Override
            public Value evaluate(Session session) {
                final Value oldValue = node.evaluate(session);
                oldValue.increment(session, "post-" + operation, increment);
                return oldValue;
            }
        };
    }
}

