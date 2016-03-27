
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse;

import com.google.common.base.Preconditions;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.Session;
import org.jsimpledb.core.Database;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.expr.Value;
import org.jsimpledb.parse.func.AbstractFunction;
import org.jsimpledb.parse.func.AllFunction;
import org.jsimpledb.parse.func.ConcatFunction;
import org.jsimpledb.parse.func.CountFunction;
import org.jsimpledb.parse.func.CreateFunction;
import org.jsimpledb.parse.func.FilterFunction;
import org.jsimpledb.parse.func.ForEachFunction;
import org.jsimpledb.parse.func.InvertFunction;
import org.jsimpledb.parse.func.LimitFunction;
import org.jsimpledb.parse.func.ListFunction;
import org.jsimpledb.parse.func.QueryCompositeIndexFunction;
import org.jsimpledb.parse.func.QueryIndexFunction;
import org.jsimpledb.parse.func.QueryListElementIndexFunction;
import org.jsimpledb.parse.func.QueryMapValueIndexFunction;
import org.jsimpledb.parse.func.QueryVersionFunction;
import org.jsimpledb.parse.func.TransformFunction;
import org.jsimpledb.parse.func.UpgradeFunction;
import org.jsimpledb.parse.func.VersionFunction;

/**
 * A {@link Session} with support for parsing Java expressions.
 */
public class ParseSession extends Session {

    private static final HashMap<String, Class<?>> PRIMITIVE_CLASSES = new HashMap<>(9);
    static {
        PRIMITIVE_CLASSES.put("void", void.class);
        PRIMITIVE_CLASSES.put("boolean", boolean.class);
        PRIMITIVE_CLASSES.put("byte", byte.class);
        PRIMITIVE_CLASSES.put("char", char.class);
        PRIMITIVE_CLASSES.put("short", short.class);
        PRIMITIVE_CLASSES.put("int", int.class);
        PRIMITIVE_CLASSES.put("float", float.class);
        PRIMITIVE_CLASSES.put("long", long.class);
        PRIMITIVE_CLASSES.put("double", double.class);
    }

    private final LinkedHashSet<String> imports = new LinkedHashSet<>();
    private final TreeMap<String, AbstractFunction> functions = new TreeMap<>();
    private final TreeMap<String, Value> variables = new TreeMap<>();

    private Parser<? extends Node> identifierParser;

// Constructors

    /**
     * Constructor for {@link org.jsimpledb.SessionMode#KEY_VALUE} mode.
     *
     * @param kvdb key/value database
     * @throws IllegalArgumentException if {@code kvdb} is null
     */
    public ParseSession(KVDatabase kvdb) {
        super(kvdb);
        this.imports.add("java.lang.*");
    }

    /**
     * Constructor for {@link org.jsimpledb.SessionMode#CORE_API} mode.
     *
     * @param db core database
     * @throws IllegalArgumentException if {@code db} is null
     */
    public ParseSession(Database db) {
        super(db);
        this.imports.add("java.lang.*");
    }

    /**
     * Constructor for {@link org.jsimpledb.SessionMode#JSIMPLEDB} mode.
     *
     * @param jdb database
     * @throws IllegalArgumentException if {@code jdb} is null
     */
    public ParseSession(JSimpleDB jdb) {
        super(jdb);
        this.imports.add("java.lang.*");
    }

// Accessors

    /**
     * Get currently configured Java imports.
     *
     * <p>
     * Each entry should of the form {@code foo.bar.Name} or {@code foo.bar.*}.
     * </p>
     *
     * @return configured imports
     */
    public Set<String> getImports() {
        return this.imports;
    }

    /**
     * Get the {@link AbstractFunction}s registered with this instance.
     *
     * @return registered functions indexed by name
     */
    public SortedMap<String, AbstractFunction> getFunctions() {
        return this.functions;
    }

    /**
     * Get all variables set on this instance.
     *
     * @return variables indexed by name
     */
    public SortedMap<String, Value> getVars() {
        return this.variables;
    }

// Function registration

    /**
     * Register the standard built-in functions such as {@code all()}, {@code foreach()}, etc.
     */
    public void registerStandardFunctions() {

        // We don't use AnnotatedClassScanner here to avoid having a dependency on the spring classes
        final Class<?>[] functionClasses = new Class<?>[] {
            AllFunction.class,
            ConcatFunction.class,
            CountFunction.class,
            CreateFunction.class,
            FilterFunction.class,
            ForEachFunction.class,
            InvertFunction.class,
            LimitFunction.class,
            ListFunction.class,
            QueryCompositeIndexFunction.class,
            QueryIndexFunction.class,
            QueryListElementIndexFunction.class,
            QueryMapValueIndexFunction.class,
            QueryVersionFunction.class,
            TransformFunction.class,
            UpgradeFunction.class,
            VersionFunction.class,
        };
        for (Class<?> cl : functionClasses)
            this.registerFunction(cl);
    }

    /**
     * Create an instance of the specified class and register it as an {@link AbstractFunction}.
     * as appropriate. The class must have a public constructor taking either a single {@link ParseSession} parameter
     * or no parameters; they will be tried in that order.
     *
     * @param cl function class
     * @return the newly instantiated function
     * @throws IllegalArgumentException if {@code cl} has no suitable constructor
     * @throws IllegalArgumentException if {@code cl} instantiation fails
     * @throws IllegalArgumentException if {@code cl} does not subclass {@link AbstractFunction}
     */
    public AbstractFunction registerFunction(Class<?> cl) {
        if (!AbstractFunction.class.isAssignableFrom(cl))
            throw new IllegalArgumentException(cl + " does not subclass " + AbstractFunction.class.getName());
        final AbstractFunction function = this.instantiate(cl.asSubclass(AbstractFunction.class));
        try {
            function.getSessionModes();
        } catch (UnsupportedOperationException e) {
            throw new IllegalArgumentException(cl + " does not know it's supported session modes", e);
        }
        this.functions.put(function.getName(), function);
        return function;
    }

    /**
     * Instantiate an instance of the given class.
     * The class must have a public constructor taking either a single {@link ParseSession} parameter
     * or no parameters; they will be tried in that order.
     */
    private <T> T instantiate(Class<T> cl) {
        Throwable failure;
        try {
            return cl.getConstructor(ParseSession.class).newInstance(this);
        } catch (NoSuchMethodException e) {
            try {
                return cl.getConstructor().newInstance();
            } catch (NoSuchMethodException e2) {
                throw new IllegalArgumentException("no suitable constructor found in class " + cl.getName());
            } catch (Exception e2) {
                failure = e2;
            }
        } catch (Exception e) {
            failure = e;
        }
        if (failure instanceof InvocationTargetException)
            failure = failure.getCause();
        throw new IllegalArgumentException("unable to instantiate class " + cl.getName() + ": " + failure, failure);
    }

// Identifier resolution

    /**
     * Get the current standalone identifier parser.
     *
     * @return current identifier parser, if any, otherwise null
     */
    public Parser<? extends Node> getIdentifierParser() {
        return this.identifierParser;
    }

    /**
     * Set the standalone identifier parser.
     *
     * <p>
     * The configured identifier parser, if any, is invoked on standalone identifiers.
     * This allows for configurable behavior with respect to resolving such identifiers.
     *
     * <p>
     * Typically when installing a new parser, the previous parser (if any) is set as its delegate.
     */
    public void setIdentifierParser(Parser<? extends Node> identifierParser) {
        this.identifierParser = identifierParser;
    }

// Class name resolution

    /**
     * Resolve a class name against this instance's currently configured class imports.
     *
     * @param name class name as it would appear in the Java language
     * @param allowPrimitive whether to allow primitive types like {@code int}
     * @param allowArray whether to allow array types like {@code Object[][]}
     * @return resolved class
     * @throws IllegalArgumentException if {@code name} cannot be resolved
     */
    public Class<?> resolveClass(final String name, boolean allowPrimitive, boolean allowArray) {

        // Strip off and count array dimensions
        int dims = 0;
        int len = name.length();
        while (len > 2 && name.charAt(len - 2) == '[' && name.charAt(len - 1) == ']') {
            dims++;
            len -= 2;
        }
        final String baseName = name.substring(0, len);

        // Parse base class and search in package imports
        final int firstDot = baseName.indexOf('.');
        final String firstPart = firstDot != -1 ? baseName.substring(0, firstDot - 1) : baseName;
        final ArrayList<String> packages = new ArrayList<>(this.imports.size() + 1);
        packages.add(null);
        packages.addAll(this.imports);
        Class<?> baseClass = null;
    search:
        for (String pkg : packages) {

            // Get absolute class name
            String className;
            if (pkg == null)
                className = baseName;
            else if (pkg.endsWith(".*"))
                className = pkg.substring(0, pkg.length() - 1) + baseName;
            else {
                if (!firstPart.equals(pkg.substring(pkg.lastIndexOf('.') + 1, pkg.length() - 2)))
                    continue;
                className = pkg.substring(0, pkg.length() - 2 - firstPart.length()) + baseName;
            }

            // Try package vs. nested classes
            while (true) {
                try {
                    baseClass = Class.forName(className, false, Thread.currentThread().getContextClassLoader());
                    break search;
                } catch (ClassNotFoundException e) {
                    // not found
                }
                final int lastDot = className.lastIndexOf('.');
                if (lastDot == -1)
                    break;
                className = className.substring(0, lastDot) + "$" + className.substring(lastDot + 1);
            }
        }
        if (baseClass == null && allowPrimitive)
            baseClass = PRIMITIVE_CLASSES.get(baseName);

        // Found?
        if (baseClass == null)
            throw new IllegalArgumentException("unknown class `" + baseName + "'");

        // Array allowed?
        if (dims > 0 && !allowArray)
            throw new IllegalArgumentException("unexpected array type `" + name + "'");

        // Apply array dimensions
        return ParseUtil.getArrayClass(baseClass, dims);
    }

    /**
     * Relativize the given class's name, so that it is as short as possible given the configured imports.
     * For example, for class {@link String} this will return {@code String}, but for class {@link ArrayList}
     * this will return {@code java.util.ArrayList} unless {@code java.util.*} has been imported.
     *
     * @param klass class whose name to relativize
     * @return relativized class name
     * @throws IllegalArgumentException if {@code klass} is null
     */
    public String relativizeClassName(Class<?> klass) {
        Preconditions.checkArgument(klass != null, "null klass");
        final String name = klass.getName();
        for (int pos = name.lastIndexOf('.'); pos > 0; pos = name.lastIndexOf('.', pos - 1)) {
            final String shortName = name.substring(pos + 1);
            try {
                if (this.resolveClass(shortName, false, true) == klass)
                    return shortName;
            } catch (IllegalArgumentException e) {
                // continue
            }
        }
        return klass.getName();
    }

// Action

    /**
     * Perform the given action in the context of this session.
     *
     * <p>
     * This is a {@link ParseSession}-specific overload of
     * {@link Session#performSessionAction Session.performSessionAction()}; see that method for details.
     *
     * @param action action to perform, possibly within a transaction
     * @return true if {@code action} completed successfully, false if a transaction could not be created
     *  or {@code action} threw an exception
     * @throws IllegalArgumentException if {@code action} is null
     */
    public boolean performParseSessionAction(final Action action) {
        return this.performSessionAction(this.wrap(action));
    }

    /**
     * Associate the current {@link org.jsimpledb.JTransaction} with this instance, if not already associated,
     * while performing the given action.
     *
     * <p>
     * This is a {@link ParseSession}-specific overload of
     * {@link Session#performSessionActionWithCurrentTransaction Session.performSessionActionWithCurrentTransaction()};
     * see that method for details.
     *
     * @param action action to perform
     * @return true if {@code action} completed successfully, false if {@code action} threw an exception
     * @throws IllegalStateException if there is a different open transaction already associated with this instance
     * @throws IllegalStateException if this instance is not in mode {@link org.jsimpledb.SessionMode#JSIMPLEDB}
     * @throws IllegalArgumentException if {@code action} is null
     */
    public boolean performParseSessionActionWithCurrentTransaction(final Action action) {
        return this.performSessionActionWithCurrentTransaction(this.wrap(action));
    }

    private Session.Action wrap(final Action action) {
        return action instanceof TransactionalAction ?
          new Session.TransactionalAction() {
            @Override
            public void run(Session session) throws Exception {
                action.run((ParseSession)session);
            }
          } :
          new Session.Action() {
            @Override
            public void run(Session session) throws Exception {
                action.run((ParseSession)session);
            }
          };
    }

    /**
     * Callback interface used by {@link ParseSession#performParseSessionAction ParseSession.performParseSessionAction()}
     * and {@link ParseSession#performParseSessionActionWithCurrentTransaction
     *  ParseSession.performParseSessionActionWithCurrentTransaction()}.
     */
    public interface Action {

        /**
         * Perform some action using the given {@link ParseSession} while a transaction is open.
         *
         * @param session session with open transaction
         * @throws Exception if an error occurs
         */
        void run(ParseSession session) throws Exception;
    }

    /**
     * Tagging interface indicating an {@link Action} that requires there to be an open transaction.
     */
    public interface TransactionalAction extends Action {
    }
}

