
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.google.common.collect.Iterables;

import java.util.Collections;

import org.dellroad.stuff.vaadin7.VaadinApplicationListener;
import org.dellroad.stuff.vaadin7.VaadinConfigurable;
import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.parse.ParseContext;
import org.jsimpledb.parse.ParseSession;
import org.jsimpledb.parse.expr.EvalException;
import org.jsimpledb.parse.expr.ExprParser;
import org.jsimpledb.parse.expr.Node;
import org.jsimpledb.parse.util.CastFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.ApplicationEventMulticaster;

/**
 * {@link JObjectContainer} whose contents are determined by a Java expression.
 */
@SuppressWarnings("serial")
@VaadinConfigurable(preConstruction = true)
public class ObjectContainer extends JObjectContainer {

    private final ParseSession session;

    private DataChangeListener dataChangeListener;
    private String contentExpression;

    @Autowired(required = false)
    @Qualifier("jsimpledbGuiEventMulticaster")
    private ApplicationEventMulticaster eventMulticaster;

    /**
     * Constructor.
     */
    public <T> ObjectContainer(JSimpleDB jdb, ParseSession session) {
        this(jdb, null, session);
    }

    /**
     * Constructor.
     *
     * @param type type restriction, or null for no restriction
     */
    public <T> ObjectContainer(JSimpleDB jdb, Class<T> type, ParseSession session) {
        super(jdb, type);
        if (session == null)
            throw new IllegalArgumentException("null session");
        this.session = session;
    }

    /**
     * Configure the expression that returns this container's content. Container will automatically reload.
     */
    public void setContentExpression(String contentExpression) {
        this.contentExpression = contentExpression;
        this.reload();
    }

    protected void doInTransaction(final Runnable action) {
        this.session.perform(new ParseSession.Action() {
            @Override
            public void run(ParseSession session) {
                action.run();
            }
        });
    }

    @Override
    protected Iterable<? extends JObject> queryForObjects() {

        // Any content?
        if (this.contentExpression == null)
            return Collections.<JObject>emptySet();

        // Parse and evaluate content expression
        final Node node = new ExprParser().parse(this.session, new ParseContext(this.contentExpression), false);
        final Object content = node.evaluate(this.session).get(this.session);
        if (!(content instanceof Iterable)) {
            throw new EvalException("expression must evaluate to an Iterable; found "
              + (content != null ? content : "null") + " instead");
        }

        // Reload container with results of expression
        return Iterables.transform((Iterable<?>)content, new CastFunction<JObject>(JObject.class));
    }

// Connectable

    @Override
    public void connect() {
        super.connect();
        if (this.eventMulticaster != null) {
            this.dataChangeListener = new DataChangeListener();
            this.dataChangeListener.register();
        }
    }

    @Override
    public void disconnect() {
        if (this.dataChangeListener != null) {
            this.dataChangeListener.unregister();
            this.dataChangeListener = null;
        }
        super.disconnect();
    }

// DataChangeListener

    private class DataChangeListener extends VaadinApplicationListener<DataChangeEvent> {

        DataChangeListener() {
            super(ObjectContainer.this.eventMulticaster);
            this.setAsynchronous(true);
        }

        @Override
        protected void onApplicationEventInternal(DataChangeEvent event) {
            ObjectContainer.this.applyChange(event.getChange());
        }
    }
}

