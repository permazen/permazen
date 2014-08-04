
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.Session;
import org.jsimpledb.core.Database;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.parse.expr.Value;
import org.jsimpledb.parse.func.AbstractFunction;

/**
 * A {@link Session} with support for parsing Java expressions.
 */
public class ParseSession extends Session {

    private final LinkedHashSet<String> imports = new LinkedHashSet<>();
    private final TreeMap<String, AbstractFunction> functions = new TreeMap<>();
    private final TreeMap<String, Value> variables = new TreeMap<>();

// Constructors

    /**
     * Constructor for core API level access.
     *
     * @param db core database
     * @throws IllegalArgumentException if {@code db} is null
     */
    public ParseSession(Database db) {
        super(db);
        this.imports.add("java.lang.*");
    }

    /**
     * Constructor for {@link JSimpleDB} level access.
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
     */
    public Set<String> getImports() {
        return this.imports;
    }

    /**
     * Get the {@link AbstractFunction}s registered with this instance.
     */
    public SortedMap<String, AbstractFunction> getFunctions() {
        return this.functions;
    }

    /**
     * Get all variables set on this instance.
     */
    public SortedMap<String, Value> getVars() {
        return this.variables;
    }

// Function registration

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

// Action

    /**
     * Perform the given action. This is a convenience method, equivalent to: {@code perform(null, action)}
     *
     * @param action action to perform
     * @throws IllegalArgumentException if {@code action} is null
     */
    public boolean perform(Action action) {
        return this.perform(null, action);
    }

    /**
     * Perform the given action within the given existing transaction, if any, otherwise within a new transaction.
     * If {@code tx} is not null, it will used and left open when this method returns. Otherwise,
     * if there is already an open transaction associated with this instance, it will be used;
     * otherwise, a new transaction is created for the duration of {@code action} and then committed.
     *
     * <p>
     * If {@code tx} is not null and there is already an open transaction associated with this instance and they
     * are not the same transaction, an {@link IllegalStateException} is thrown.
     * </p>
     *
     * @param tx transaction in which to perform the action, or null to create a new one (if necessary)
     * @param action action to perform
     * @throws IllegalStateException if {@code tx} conflict with the already an open transaction associated with this instance
     * @throws IllegalArgumentException if {@code action} is null
     */
    public boolean perform(Transaction tx, final Action action) {
        return this.perform(tx, new Session.Action() {
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
         */
        void run(ParseSession session) throws Exception;
    }
}

