
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli.parse.expr;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
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
import java.util.List;
import java.util.regex.Matcher;

import org.jsimpledb.cli.ObjInfo;
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
import org.jsimpledb.core.ObjType;
import org.jsimpledb.core.SetField;
import org.jsimpledb.core.SimpleField;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.util.ParseContext;

public class BaseExprParser implements Parser<Node> {

    public static final BaseExprParser INSTANCE = new BaseExprParser();

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Node parse(Session session, ParseContext ctx, boolean complete) {

        // Parse atom
        final Node atom = AtomParser.INSTANCE.parse(session, ctx, complete);

        // Parse operator, if any
        final Matcher opMatcher = ctx.tryPattern("\\s*(\\[|\\.|\\(|\\+{2}|-{2})");
        if (opMatcher == null)
            return atom;
        final String opsym = opMatcher.group(1);
        final int mark = ctx.getIndex();

        // Handle operators
        switch (opsym) {
        case "(":
        {
            // Atom must be an identifier, for a function call
            if (!(atom instanceof IdentNode))
                throw new ParseException(ctx);
            final String functionName = ((IdentNode)atom).getName();
            final Function function = session.getFunctions().get(functionName);
            if (function != null) {
                final Object params = function.parseParams(session, ctx, complete);
                return new Node() {
                    @Override
                    public Value evaluate(Session session) {
                        return function.apply(session, params);
                    }
                };
            }
            throw new ParseException(ctx, "unknown function `" + functionName + "()'")
              .addCompletions(ParseUtil.complete(session.getFunctions().keySet(), functionName));
        }
        case ".":
        {
            // Parse next atom - it must be an identifier, method or property name
            this.spaceParser.parse(ctx, complete);
            final Node memberNode = AtomParser.INSTANCE.parse(session, ctx, complete);
            if (!(memberNode instanceof IdentNode)) {
                ctx.setIndex(mark);
                throw new ParseException(ctx);
            }
            String member = ((IdentNode)memberNode).getName();

            // If first atom was an identifier, must be a class name, with last component the field or method name
            final Class<?> cl;
            if (atom instanceof IdentNode) {

                // Parse class name
                String className = ((IdentNode)atom).getName();
                for (Matcher matcher; (matcher = ctx.tryPattern("\\s*\\.\\s*(" + IdentNode.NAME_PATTERN + ")")) != null; ) {
                    className += "." + member;
                    member = matcher.group(1);
                }

                // Resolve class
                if ((cl = session.resolveClass(className)) == null)
                    throw new ParseException(ctx, "unknown class `" + className + "'");     // TODO: tab-completions
            } else
                cl = null;

            // Handle property access
            if (ctx.tryPattern("\\s*\\(") == null) {
                final String propertyName = member;
                return new Node() {
                    @Override
                    public Value evaluate(Session session) {
                        if (cl != null) {
                            return new Value(propertyName.equals("class") ?
                              cl : BaseExprParser.this.readStaticField(cl, propertyName));
                        }
                        return BaseExprParser.this.evaluateProperty(session, atom.evaluate(session), propertyName);
                    }
                };
            }

            // Handle method call
            return this.createMethodInvokeNode(cl != null ? cl : atom, member, BaseExprParser.parseParams(session, ctx, complete));
        }
        case "[":
        {
            this.spaceParser.parse(ctx, complete);
            final Node index = AssignmentExprParser.INSTANCE.parse(session, ctx, complete);
            this.spaceParser.parse(ctx, complete);
            if (!ctx.tryLiteral("]"))
                throw new ParseException(ctx).addCompletion("] ");
            return new Node() {
                @Override
                public Value evaluate(Session session) {
                    return Op.ARRAY_ACCESS.apply(session, atom.evaluate(session), index.evaluate(session));
                }
            };
        }
        case "++":
            return this.createPostcrementNode("increment", atom, true);
        case "--":
            return this.createPostcrementNode("decrement", atom, false);
        default:
            throw new RuntimeException("internal error: " + opsym);
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

        // Evaluate value
        final Object obj = value.checkNotNull(session, "property `" + name + "' access");
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
                        throw new EvalException("error writing property `" + name + "' to object of type "
                          + cl.getName() + ": " + e, e);
                    }
                }
            } : null) {
                @Override
                public Object get(Session session) {
                    try {
                        return readMethod.invoke(obj);
                    } catch (Exception e) {
                        throw new EvalException("error reading property `" + name + "' from object of type "
                          + cl.getName() + ": " + e, e);
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

        // Handle properties of database objects
        if (obj instanceof ObjId) {

            // Get object info
            final ObjId id = (ObjId)obj;
            final ObjInfo info = ObjInfo.getObjInfo(session, id);
            if (info == null)
                throw new EvalException("error reading field `" + name + "': object " + id + " does not exist");
            final ObjType objType = info.getObjType();

            // Find the field
            final org.jsimpledb.core.Field<?> field = Iterables.find(objType.getFields().values(),
              new Predicate<org.jsimpledb.core.Field<?>>() {
                @Override
                public boolean apply(org.jsimpledb.core.Field<?> field) {
                    return field.getName().equals(name);
                }
              }, null);
            if (field == null)
                throw new EvalException("error reading field `" + name + "': there is no such field in " + objType);

            // Return value
            final Transaction tx = session.getTransaction();
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
                final String desc;
                if (target instanceof Node) {       // instance method
                    obj = ((Node)target).evaluate(session).checkNotNull(session, "method " + name + "() invocation");
                    cl = obj.getClass();
                    desc = "object of type " + cl.getName();
                } else {                            // static method
                    obj = null;
                    cl = (Class<?>)target;
                    desc = cl.toString();
                }
                final Object[] params = Lists.transform(paramNodes, new com.google.common.base.Function<Node, Object>() {
                    @Override
                    public Object apply(Node param) {
                        return param.evaluate(session).get(session);
                    }
                }).toArray();
                for (Method method : cl.getMethods()) {
                    if (!method.getName().equals(name))
                        continue;
                    final Class<?>[] ptypes = method.getParameterTypes();
                    Object[] methodParams = params;
                    if (method.isVarArgs()) {
                        if (params.length < ptypes.length - 1)
                            continue;
                        methodParams = new Object[ptypes.length];
                        System.arraycopy(params, 0, methodParams, 0, ptypes.length - 1);
                        Object[] varargs = new Object[params.length - (ptypes.length - 1)];
                        System.arraycopy(params, ptypes.length - 1, varargs, 0, varargs.length);
                        methodParams[ptypes.length - 1] = varargs;
                    } else if (methodParams.length != ptypes.length)
                        continue;
                    try {
                        return new Value(method.invoke(obj, methodParams));
                    } catch (IllegalArgumentException e) {
                        continue;                               // a parameter type didn't match -> wrong method
                    } catch (InvocationTargetException e) {
                        throw new EvalException("got exception invoking method `" + name + "()' "
                          + desc + ": " + e, e.getTargetException());
                    } catch (Exception e) {
                        throw new EvalException("error invoking method `" + name + "()' " + desc + ": " + e, e);
                    }
                }
                throw new EvalException("no compatible method `" + name + "()' found in " + cl);
            }
        };
    }

    private Node createPostcrementNode(final String operation, final Node atom, final boolean increment) {
        return new Node() {
            @Override
            public Value evaluate(Session session) {
                final Value oldValue = atom.evaluate(session);
                oldValue.increment(session, "post-" + operation, increment);
                return oldValue;
            }
        };
    }
}

