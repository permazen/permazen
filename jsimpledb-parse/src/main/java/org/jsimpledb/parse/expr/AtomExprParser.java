
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse.expr;

import java.beans.BeanInfo;
import java.beans.IndexedPropertyDescriptor;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.jsimpledb.parse.ParseException;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.ParseUtil;
import org.jsimpledb.parse.Parser;
import org.jsimpledb.parse.SpaceParser;
import org.jsimpledb.parse.func.Function;
import org.jsimpledb.util.ParseContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;

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
            final String className = ctx.matchPrefix(LiteralExprParser.IDENTS_AND_DOTS_PATTERN).group().replaceAll("\\s+", "");
            final ClassNode classNode = ClassNode.parse(ctx, className, true);

            // Handle tab-completion
            if (complete && ctx.isEOF()) {
                try {
                    session.resolveClass(className, true);
                } catch (IllegalArgumentException e) {
                    throw new ParseException(ctx).addCompletions(AtomExprParser.getClassNameCompletions(session, className));
                }
            }
            if (className.equals("void"))
                throw new ParseException(ctx, "illegal instantiation of void");

            // Get array dimensions, if any, including length initialization expressions
            final ArrayList<Node> dimensions = new ArrayList<>();
            boolean sawNull = false;
            while (true) {
                ctx.skipWhitespace();
                if (!ctx.tryLiteral("["))
                    break;
                ctx.skipWhitespace();
                if (ctx.tryLiteral("]")) {
                    dimensions.add(null);
                    sawNull = true;
                    continue;
                }
                if (!sawNull) {
                    dimensions.add(ExprParser.INSTANCE.parse(session, ctx, complete));
                    ctx.skipWhitespace();
                }
                if (!ctx.tryLiteral("]"))
                    throw new ParseException(ctx, "expected `]'").addCompletion("]");
            }
            final int numDimensions = dimensions.size();

            // Handle non-array instantiation
            if (numDimensions == 0) {

                // Parse parameters
                if (!ctx.tryLiteral("("))
                    throw new ParseException(ctx, "expected `('").addCompletion("(");
                ctx.skipWhitespace();
                final List<Node> paramNodes = AtomExprParser.parseParams(session, ctx, complete);

                // Return constructor invocation node
                return new ConstructorInvokeNode(classNode, paramNodes);
            }

            // Check number of dimensions
            if (numDimensions > 255)
                throw new ParseException(ctx, "too many array dimensions (" + numDimensions + " > 255)");

            // Handle empty or initialized array instantiation expression
            return dimensions.get(0) != null ?
              new EmptyArrayNode(classNode, dimensions) :
              new LiteralArrayNode(classNode, numDimensions,
               LiteralArrayNode.parseArrayLiteral(session, ctx, complete, numDimensions));
        } else
            ctx.setIndex(start);

        // Handle session function invocation - distguished by a single identifier, followed by '('
        final Matcher functionMatcher = ctx.tryPattern("(" + ParseUtil.IDENT_PATTERN + ")\\s*\\(");
        if (functionMatcher != null) {

            // Find function
            final String functionName = functionMatcher.group(1);
            final Function function = session.getFunctions().get(functionName);
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

                @Override
                public Class<?> getType(ParseSession session) {
                    return Object.class;
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
        final Matcher methodMatcher = ctx.tryPattern(
          "(" + LiteralExprParser.CLASS_NAME_PATTERN + ")\\s*::\\s*(" + ParseUtil.IDENT_PATTERN + ")");
        if (methodMatcher != null) {

            // Parse class and method name
            final ClassNode classNode = ClassNode.parse(ctx, methodMatcher.group(1), false);
            final String methodName = methodMatcher.group(2);

            // Support tab-completion of method names
            if (complete && ctx.isEOF()) {
                Class<?> type = null;
                try {
                    type = classNode.resolveClass(session);
                } catch (IllegalArgumentException e) {
                    // ignore
                }
                if (type != null) {
                    final HashSet<String> validMethodNames = new HashSet<>();
                    validMethodNames.add("new");
                    for (Method method : type.getMethods())
                        validMethodNames.add(method.getName());
                    if (!validMethodNames.contains(methodName)) {
                        throw new ParseException(ctx, "unknown method `" + methodName + "()' in " + type)
                          .addCompletions(ParseUtil.complete(validMethodNames, methodName));
                    }
                }
            }

            // Return unbound method reference
            return new UnboundMethodReferenceNode(classNode, methodName);
        }

        // Handle tab-completion of "Foo" and "Foo.", assuming "Foo" is a class name (or prefix of a class name)
        if (complete) {
            final Matcher nameDotMatcher = ctx.tryPattern("(" + ParseUtil.IDENT_PATTERN + ")\\s*(\\.\\s*)?$");
            if (nameDotMatcher != null) {
                final String className = nameDotMatcher.group(1);
                if (nameDotMatcher.group(2) == null)
                    throw new ParseException(ctx).addCompletions(AtomExprParser.getClassNameCompletions(session, className));
                try {
                    final Class<?> clazz = session.resolveClass(className, false);
                    throw new ParseException(ctx).addCompletions(AtomExprParser.getStaticMemberCompletions(clazz, ""));
                } catch (IllegalArgumentException e) {
                    ctx.setIndex(start);            // class not found
                }
            }
        }

        // Handle static method invocation and field access
        final Matcher staticMatcher = ctx.tryPattern(
          "(" + ParseUtil.IDENT_PATTERN + ")\\s*\\.\\s*(" + ParseUtil.IDENT_PATTERN + ")");
        if (staticMatcher != null) {

            // Parse first two identifiers
            final StringBuilder idents = new StringBuilder(staticMatcher.group(1));     // class name so far
            String member = staticMatcher.group(2);                                     // class member name

            // Keep parsing identifiers as long as we can; after each identifier, try to resolve a class name
            final ArrayList<Integer> indexList = new ArrayList<>();
            final ArrayList<Class<?>> classList = new ArrayList<>();
            final ArrayList<String> memberList = new ArrayList<>();
            Class<?> cl;
            while (true) {
                try {
                    cl = session.resolveClass(idents.toString(), false);
                } catch (IllegalArgumentException e) {
                    cl = null;
                }
                classList.add(cl);
                indexList.add(ctx.getIndex());
                memberList.add(member);
                final Matcher matcher = ctx.tryPattern("\\s*\\.\\s*(" + ParseUtil.IDENT_PATTERN + ")");
                if (matcher == null)
                    break;
                idents.append('.').append(member);
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

            // Check if class was found; if not, handle tab-completion of class name
            if (cl == null) {
                final ParseException e = new ParseException(ctx, "unknown class `" + idents + "'");
                if (complete && ctx.isEOF())
                    e.addCompletions(AtomExprParser.getClassNameCompletions(session, idents.toString()));
                throw e;
            }

            // Handle tab-completion of static class members
            if (complete && ctx.isEOF())
                throw new ParseException(ctx).addCompletions(AtomExprParser.getStaticMemberCompletions(cl, member));

            // Handle static method invocation
            if (ctx.tryPattern("\\s*\\(") != null)
                return new MethodInvokeNode(cl, member, AtomExprParser.parseParams(session, ctx, complete));

            // Handle static field access
            try {
                return new ConstNode(new StaticFieldValue(AtomExprParser.findField(cl, member, true)));
            } catch (NoSuchFieldException e) {
                throw new ParseException(ctx, e.getMessage());
            }
        }

        // Can't parse this
        throw new ParseException(ctx);
    }

    // Find field
    static Field findField(Class<?> cl, String fieldName, boolean isStatic) throws NoSuchFieldException {
        final Field bestMatch = AtomExprParser.findField(cl, fieldName, null, isStatic);
        if (bestMatch == null)
            throw new NoSuchFieldException("class `" + cl.getName() + "' has no field named `" + fieldName + "'");
        if (((bestMatch.getModifiers() & Modifier.STATIC) != 0) != isStatic) {
            throw new NoSuchFieldException("field `" + fieldName
              + "' in class `" + cl.getName() + "' is not " + (isStatic ? "a static" : "an instance") + " field");
        }
        try {
            bestMatch.setAccessible(true);
        } catch (SecurityException e) {
            // ignore
        }
        return bestMatch;
    }

    // Helper method for findField()
    private static Field findField(Class<?> cl, String fieldName, Field bestSoFar, boolean isStatic) {

        // Find field with the given name declared in class, if any
        final int mask = Modifier.STATIC;
        final int flags = isStatic ? Modifier.STATIC : 0;
        for (Field candidate : cl.getDeclaredFields()) {
            if (!candidate.getName().equals(fieldName))
                continue;
            if (bestSoFar == null || (bestSoFar.getModifiers() & mask) != flags)
                bestSoFar = candidate;
            if (bestSoFar != null && (bestSoFar.getModifiers() & mask) == flags)
                return bestSoFar;
            break;
        }

        // Recurse on supertypes
        if (cl.getSuperclass() != null)
            bestSoFar = AtomExprParser.findField(cl.getSuperclass(), fieldName, bestSoFar, isStatic);
        for (Class<?> iface : cl.getInterfaces())
            bestSoFar = AtomExprParser.findField(iface, fieldName, bestSoFar, isStatic);

        // Done
        return bestSoFar;
    }

    /**
     * Get all class name completions for {@code some.class.Name...}.
     */
    static Iterable<String> getClassNameCompletions(ParseSession session, String name) {

        // Use a separate class to avoid exception if Spring classes are not available
        try {
            return new ClassNameCompletion(name).findCompletions(session);
        } catch (NoClassDefFoundError e) {
            return Collections.emptySet();
        }
    }

    /**
     * Get all public static member name completions for {@code some.class.Name.member...}.
     */
    static Iterable<String> getStaticMemberCompletions(Class<?> cl, String name) {
        final TreeSet<String> names = new TreeSet<>();
        AtomExprParser.getMemberNames(cl, names, true);
        names.add("class");
        return ParseUtil.complete(names, name);
    }

    /**
     * Get all public instance member name completions, including bean property names, for {@code some.class.Name.member...}.
     */
    static Iterable<String> getInstanceMemberCompletions(Class<?> cl, String name) {
        final TreeSet<String> names = new TreeSet<>();
        AtomExprParser.getMemberNames(cl, names, false);
        AtomExprParser.getBeanPropertyNames(cl, names);
        if (cl.isArray())
            names.add("length");
        return ParseUtil.complete(names, name);
    }

    private static Iterable<String> getMemberNames(Class<?> cl, TreeSet<String> names, boolean isStatic) {

        // Add public and private fields
        int mask = Modifier.STATIC;
        int flags = isStatic ? Modifier.STATIC : 0;
        for (Field field : cl.getDeclaredFields()) {
            if ((field.getModifiers() & mask) != flags)
                continue;
            names.add(field.getName());
        }

        // Add public methods only
        mask |= Modifier.PUBLIC;
        flags |= Modifier.PUBLIC;
        for (Method method : cl.getDeclaredMethods()) {
            if ((method.getModifiers() & mask) != flags)
                continue;
            names.add(method.getName() + "(");
        }

        // Recurse on supertypes
        if (cl.getSuperclass() != null)
            AtomExprParser.getMemberNames(cl.getSuperclass(), names, isStatic);
        for (Class<?> iface : cl.getInterfaces())
            AtomExprParser.getMemberNames(iface, names, isStatic);

        // Done
        return names;
    }

    // Parse method parameters; we assume opening `(' has just been parsed
    static List<Node> parseParams(ParseSession session, ParseContext ctx, boolean complete) {
        final ArrayList<Node> params = new ArrayList<>();
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

    // Get bean property names
    static void getBeanPropertyNames(Class<?> cl, TreeSet<String> names) {
        final BeanInfo beanInfo;
        try {
            beanInfo = Introspector.getBeanInfo(cl);
        } catch (IntrospectionException e) {
            return;
        }
        for (PropertyDescriptor propertyDescriptor : beanInfo.getPropertyDescriptors()) {
            if (propertyDescriptor instanceof IndexedPropertyDescriptor)
                continue;
            if (propertyDescriptor.getReadMethod() == null)
                continue;
            names.add(propertyDescriptor.getName());
        }
    }

// ClassNameCompletion

    private static class ClassNameCompletion {

        private final String prefix;
        private final boolean hasDot;

        ClassNameCompletion(String prefix) {
            this.prefix = prefix;
            this.hasDot = prefix.indexOf('.') != -1;
        }

        public Iterable<String> findCompletions(ParseSession session) {

            // Create search patterns
            final HashSet<String> patterns = new HashSet<>();
            if (!this.hasDot) {
                for (String mport : session.getImports()) {
                    mport = mport.replace('.', '/');
                    if (mport.length() > 1 && mport.charAt(mport.length() - 2) == '/' && mport.charAt(mport.length() - 1) == '*') {
                        patterns.add(ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX
                          + mport.substring(0, mport.length() - 1) + this.prefix + "*.class");
                    } else if (mport.lastIndexOf('/') != -1 && mport.substring(mport.lastIndexOf('/') + 1).startsWith(this.prefix))
                        patterns.add(ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX + mport + ".class");
                }
            } else {
                String path = this.prefix.replace('.', '/');
                patterns.add(ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX + path + "*.class");
            }

            // Search for matching class resources
            final HashSet<String> classNames = new HashSet<>();
            for (String pattern : patterns) {
                final Resource[] resources;
                try {
                    resources = new PathMatchingResourcePatternResolver().getResources(pattern);
                } catch (IOException e) {
                    continue;
                }
                for (Resource resource : resources) {
                    String name;
                    if (this.hasDot) {
                        final String uri;
                        try {
                            uri = resource.getURI().toString();
                        } catch (IOException e) {
                            continue;
                        }
                        final int npos = uri.lastIndexOf(this.prefix.replace('.', '/'));
                        name = uri.substring(npos).replace('/', '.');
                    } else {
                        name = resource.getFilename();
                    }
                    if (name.endsWith(".class"))                                // should always be the case
                        name = name.substring(0, name.length() - 6);
                    final int dot = name.indexOf('.', this.prefix.length());
                    if (dot != -1)
                        name = name.substring(0, dot);                          // stop at '.' separators
                    final int dollar = name.indexOf('$', this.prefix.length());
                    if (dollar != -1)
                        name = name.substring(0, dollar);                       // stop at '$' separators
                    classNames.add(name);
                }
            }
            return ParseUtil.complete(classNames, this.prefix);
        }
    }
}
