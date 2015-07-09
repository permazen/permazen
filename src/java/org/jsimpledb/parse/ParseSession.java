
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.parse;

import com.google.common.base.Preconditions;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.Session;
import org.jsimpledb.core.Database;
import org.jsimpledb.kv.KVDatabase;
import org.jsimpledb.parse.expr.Value;
import org.jsimpledb.parse.func.AbstractFunction;
import org.jsimpledb.parse.func.AllFunction;
import org.jsimpledb.parse.func.ConcatFunction;
import org.jsimpledb.parse.func.CountFunction;
import org.jsimpledb.parse.func.CreateFunction;
import org.jsimpledb.parse.func.FilterFunction;
import org.jsimpledb.parse.func.ForEachFunction;
import org.jsimpledb.parse.func.Function;
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

    private final LinkedHashSet<String> imports = new LinkedHashSet<>();
    private final TreeMap<String, AbstractFunction> functions = new TreeMap<>();
    private final TreeMap<String, Value> variables = new TreeMap<>();

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
        for (Class<?> cl : functionClasses) {
            final Function annotation = cl.getAnnotation(Function.class);
            if (annotation != null && Arrays.asList(annotation.modes()).contains(this.getMode()))
                this.registerFunction(cl);
        }
    }

    /**
     * Create an instance of the specified class and register it as an {@link AbstractFunction}.
     * as appropriate. The class must have a public constructor taking either a single {@link ParseSession} parameter
     * or no parameters; they will be tried in that order.
     *
     * @param cl function class
     * @throws IllegalArgumentException if {@code cl} has no suitable constructor
     * @throws IllegalArgumentException if {@code cl} instantiation fails
     * @throws IllegalArgumentException if {@code cl} does not subclass {@link AbstractFunction}
     */
    public void registerFunction(Class<?> cl) {
        if (!AbstractFunction.class.isAssignableFrom(cl))
            throw new IllegalArgumentException(cl + " does not subclass " + AbstractFunction.class.getName());
        final AbstractFunction function = this.instantiate(cl.asSubclass(AbstractFunction.class));
        this.functions.put(function.getName(), function);
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

// Class name resolution

    /**
     * Resolve a class name against this instance's currently configured class imports.
     *
     * @param name class name
     * @return resolved class, or null if not found
     */
    public Class<?> resolveClass(final String name) {
        final int firstDot = name.indexOf('.');
        final String firstPart = firstDot != -1 ? name.substring(0, firstDot - 1) : name;
        final ArrayList<String> packages = new ArrayList<>(this.imports.size() + 1);
        packages.add(null);
        packages.addAll(this.imports);
        for (String pkg : packages) {

            // Get absolute class name
            String className;
            if (pkg == null)
                className = name;
            else if (pkg.endsWith(".*"))
                className = pkg.substring(0, pkg.length() - 1) + name;
            else {
                if (!firstPart.equals(pkg.substring(pkg.lastIndexOf('.') + 1, pkg.length() - 2)))
                    continue;
                className = pkg.substring(0, pkg.length() - 2 - firstPart.length()) + name;
            }

            // Try package vs. nested classes
            while (true) {
                try {
                    return Class.forName(className, false, Thread.currentThread().getContextClassLoader());
                } catch (ClassNotFoundException e) {
                    // not found
                }
                final int lastDot = className.lastIndexOf('.');
                if (lastDot == -1)
                    break;
                className = className.substring(0, lastDot) + "$" + className.substring(lastDot + 1);
            }
        }
        return null;
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
            if (this.resolveClass(shortName) == klass)
                return shortName;
        }
        return klass.getName();
    }

// Action

    /**
     * Perform the given action within a new transaction associated with this instance.
     *
     * <p>
     * If {@code action} throws an {@link Exception}, it will be caught and handled by {@link #reportException reportException()}
     * and then false returned.
     *
     * @param action action to perform
     * @return true if {@code action} completed successfully, false if the transaction could not be created
     *  or {@code action} threw an exception
     * @throws IllegalArgumentException if {@code action} is null
     * @throws IllegalStateException if there is already an open transaction associated with this instance
     */
    public boolean perform(final Action action) {
        return this.perform(new Session.Action() {
            @Override
            public void run(Session session) throws Exception {
                action.run((ParseSession)session);
            }
        });
    }

    /**
     * Associate the current {@link org.jsimpledb.JTransaction} with this instance while performing the given action.
     *
     * <p>
     * If {@code action} throws an {@link Exception}, it will be caught and handled by {@link #reportException reportException()}
     * and then false returned.
     *
     * <p>
     * There must be a {@link org.jsimpledb.JTransaction} open and
     * {@linkplain org.jsimpledb.JTransaction#getCurrent associated with the current thread}.
     * It will be left open when this method returns.
     *
     * <p>
     * This method safely handles re-entrant invocation.
     *
     * @param action action to perform
     * @return true if {@code action} completed successfully, false if {@code action} threw an exception
     * @throws IllegalArgumentException if {@code action} is null
     * @throws IllegalStateException if there is already an open transaction associated with this instance
     * @throws IllegalStateException if this instance is not in mode {@link org.jsimpledb.SessionMode#JSIMPLEDB}
     */
    public boolean performWithCurrentTransaction(final Action action) {
        return this.performWithCurrentTransaction(new Session.Action() {
            @Override
            public void run(Session session) throws Exception {
                action.run((ParseSession)session);
            }
        });
    }

    /**
     * Callback interface used by {@link ParseSession#perform ParseSession.perform()}.
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
}

