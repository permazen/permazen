
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.ParseUtil;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.SpaceParser;
import org.jsimpledb.parse.func.AbstractFunction;

/**
 * Parses atomic Java expressions such as parenthesized expressions, {@code new} expressions,
 * session function calls, identifiers (e.g., lambda method parameter names), and literals.
 *
 * <p>
 * Includes these extensions:
 * <ul>
 *  <li>Access to Java bean methods via "property syntax", e.g., {@code foo.name = "fred"} means {@code foo.setName("fred")}</li>
 *  <li>Access database object fields via "property syntax" given an object ID, e.g., <code>@fc21bf6d8930a215.name</code></li>
 * </ul>
 */
public class AtomExprParser implements Parser<Node> {

    public static final AtomExprParser INSTANCE = new AtomExprParser();

    private final SpaceParser spaceParser = new SpaceParser();

    @Override
    public Node parse(ParseSession session, ParseContext ctx, boolean complete) {

        // Note starting point for back-tracking
        final int start = ctx.getIndex();

        // Handle parenthesized expression
        if (ctx.tryLiteral("(")) {
            this.spaceParser.parse(ctx, complete);
            final Node node = ExprParser.INSTANCE.parse(session, ctx, complete);
            this.spaceParser.parse(ctx, complete);
            if (!ctx.tryLiteral(")"))
                throw new ParseException(ctx).addCompletion(") ");
            return node;
        }

        // Handle literals
        try {
            return LiteralExprParser.INSTANCE.parse(session, ctx, complete);
        } catch (ParseException e) {
            ctx.setIndex(start);
        }

        // Handle "new" expressions
        if (ctx.tryPattern("new(?!\\p{javaJavaIdentifierPart})") != null) {

            // Get base class name
            new SpaceParser(true).parse(ctx, complete);
            final Class<?> baseClass = LiteralExprParser.parseClassName(session, ctx, complete);
            if (baseClass == void.class)
                throw new ParseException(ctx, "illegal instantiation of void");

            // Get array dimensions, if any, including length initialization expressions
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

            // Handle non-array instantiation
            if (dims.isEmpty()) {

                // Parse parameters
                if (!ctx.tryLiteral("("))
                    throw new ParseException(ctx, "expected `('").addCompletion("(");
                ctx.skipWhitespace();
                final List<Node> paramNodes = AtomExprParser.parseParams(session, ctx, complete);

                // Return constructor invocation node
                return new ConstructorInvokeNode(baseClass, paramNodes);
            }

            // Check number of dimensions
            if (dims.size() > 255)
                throw new ParseException(ctx, "too many array dimensions (" + dims.size() + " > 255)");

            // Handle empty or initialized array instantiation expression
            final Class<?> elemType = ParseUtil.getArrayClass(baseClass, dims.size() - 1);
            if (dims.get(0) != null)
                return new EmptyArrayNode(elemType, dims);
            else
                return new LiteralArrayNode(elemType, LiteralArrayNode.parseArrayLiteral(session, ctx, complete, elemType));
        } else
            ctx.setIndex(start);

        // Handle session function invocation - distguished by a single identifier, followed by '('
        final Matcher functionMatcher = ctx.tryPattern("(" + IdentNode.NAME_PATTERN + ")\\s*\\(");
        if (functionMatcher != null) {

            // Find function
            final String functionName = functionMatcher.group(1);
            final AbstractFunction function = session.getFunctions().get(functionName);
            if (function == null) {
                throw new ParseException(ctx, "unknown function `" + functionName + "()'")
                  .addCompletions(ParseUtil.complete(session.getFunctions().keySet(), functionName));
            }

            // Parse function parameters
            ctx.skipWhitespace();
            final Object params = function.parseParams(session, ctx, complete);

            // Return node that applies the function to the parameters
            return new Node() {
                @Override
                public Value evaluate(ParseSession session) {
                    return function.apply(session, params);
                }
            };
        }

        // Handle plain identifier - delegate to standalone identifier parser, if any
        final Parser<? extends Node> identifierParser = session.getIdentifierParser();
        if (identifierParser != null) {
            try {
                final Node node = identifierParser.parse(session, ctx, complete);
                ctx.skipWhitespace();
                return node;
            } catch (ParseException e) {
                if (complete && ctx.isEOF())
                    throw e;
                ctx.setIndex(start);
            }
        }

        // Handle unbound method references
        final Matcher methodMatcher = ctx.tryPattern(LiteralExprParser.CLASS_NAME_PATTERN + "\\s*::\\s*" + IdentNode.NAME_PATTERN);
        if (methodMatcher != null) {

            // Parse class and method name
            ctx.setIndex(start);
            final Class<?> cl = LiteralExprParser.parseArrayClassName(session, ctx, complete);
            final String methodName = ctx.matchPrefix("\\s*::\\s*(" + IdentNode.NAME_PATTERN + ")").group(1);

            // Verify method exists, and support tab-completion if not
            final HashSet<String> validMethodNames = new HashSet<>();
            validMethodNames.add("new");
            for (Method method : cl.getMethods())
                validMethodNames.add(method.getName());
            if (!validMethodNames.contains(methodName)) {
                throw new ParseException(ctx, "unknown method `" + methodName + "()' in " + cl)
                  .addCompletions(ParseUtil.complete(validMethodNames, methodName));
            }

            // Return unbound method reference
            return new UnboundMethodReferenceNode(cl, methodName);
        }

        // Handle static method invocation and field access
        final Matcher staticMatcher = ctx.tryPattern(IdentNode.NAME_PATTERN + "\\s*\\.\\s*" + IdentNode.NAME_PATTERN);
        if (staticMatcher != null) {

            // Parse first two identifiers
            ctx.setIndex(start);
            String idents = ctx.matchPrefix(IdentNode.NAME_PATTERN).group();            // class name so far
            ctx.matchPrefix("\\s*\\.\\s*");
            String member = ctx.matchPrefix(IdentNode.NAME_PATTERN).group();            // class member name

            // Keep parsing identifiers as long as we can; after each identifier, try to resolve a class name
            final ArrayList<Integer> indexList = new ArrayList<>();
            final ArrayList<Class<?>> classList = new ArrayList<>();
            final ArrayList<String> memberList = new ArrayList<>();
            Class<?> cl;
            while (true) {
                try {
                    cl = session.resolveClass(idents, false, false);
                } catch (IllegalArgumentException e) {
                    cl = null;
                }
                classList.add(cl);
                indexList.add(ctx.getIndex());
                memberList.add(member);
                final Matcher matcher = ctx.tryPattern("\\s*\\.\\s*(" + IdentNode.NAME_PATTERN + ")");
                if (matcher == null)
                    break;
                idents += "." + member;
                member = matcher.group(1);
            }
            ctx.skipWhitespace();

            // Use the longest class name resolved, if any
            for (int i = classList.size() - 1; i >= 0; i--) {
                if ((cl = classList.get(i)) != null) {
                    ctx.setIndex(indexList.get(i));
                    member = memberList.get(i);
                    break;
                }
            }
            if (cl == null)
                throw new ParseException(ctx, "unknown class `" + idents + "'");        // TODO: tab-completions

            // Handle static method invocation
            if (ctx.tryPattern("\\s*\\(") != null)
                return new MethodInvokeNode(cl, member, AtomExprParser.parseParams(session, ctx, complete));

            // Handle static field access
            return new ConstNode(new StaticFieldValue(this.findStaticField(ctx, cl, member, complete)));
        }

        // Can't parse this
        throw new ParseException(ctx);
    }

    // Find static field
    private Field findStaticField(ParseContext ctx, Class<?> cl, String fieldName, boolean complete) {
        final Field[] holder = new Field[1];
        try {
            this.findStaticField(ctx, cl, fieldName, holder);
            final Field field = holder[0];
            if (field == null)
                throw new ParseException(ctx, "class `" + cl.getName() + "' has no field named `" + fieldName + "'");
            if ((field.getModifiers() & Modifier.STATIC) == 0)
                throw new ParseException(ctx, "field `" + fieldName + "' in class `" + cl.getName() + "' is not a static field");
            return field;
        } catch (ParseException e) {
            throw complete ? e.addCompletions(this.getStaticNameCompletions(cl, fieldName)) : e;
        }
    }

    // Helper method for findStaticField()
    private void findStaticField(ParseContext ctx, Class<?> cl, String fieldName, Field[] holder) {

        // Find field with the given name declared in class, if any
        for (Field field : cl.getDeclaredFields()) {
            if (field.getName().equals(fieldName)) {
                if (holder[0] == null || (holder[0].getModifiers() & Modifier.STATIC) == 0)
                    holder[0] = field;
                else {
                    throw new ParseException(ctx, "field `" + fieldName + "' in class `" + cl.getName()
                      + "' is ambiguous, found in both " + holder[0].getDeclaringClass() + " and " + cl);
                }
                break;
            }
        }

        // Recurse on supertypes
        if (cl.getSuperclass() != null)
            this.findStaticField(ctx, cl.getSuperclass(), fieldName, holder);
        for (Class<?> iface : cl.getInterfaces())
            this.findStaticField(ctx, iface, fieldName, holder);
    }

    // Get all completions for some.class.Name.foo...
    private Iterable<String> getStaticNameCompletions(Class<?> cl, String name) {
        final TreeSet<String> names = new TreeSet<>();
        this.getStaticPropertyNames(cl, names);
        names.add("class");
        return ParseUtil.complete(this.getStaticPropertyNames(cl, names), name);
    }

    // Helper method for getStaticNameCompletions()
    private Iterable<String> getStaticPropertyNames(Class<?> cl, TreeSet<String> names) {

        // Add static field and method names
        for (Field field : cl.getDeclaredFields()) {
            if ((field.getModifiers() & Modifier.STATIC) != 0)
                names.add(field.getName());
        }
        for (Method method : cl.getDeclaredMethods()) {
            if ((method.getModifiers() & Modifier.STATIC) != 0)
                names.add(method.getName());
        }

        // Recurse on supertypes
        if (cl.getSuperclass() != null)
            this.getStaticPropertyNames(cl.getSuperclass(), names);
        for (Class<?> iface : cl.getInterfaces())
            this.getStaticPropertyNames(iface, names);

        // Done
        return names;
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
}
