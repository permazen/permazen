
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.google.common.collect.Iterables;

import org.dellroad.stuff.spring.RetryTransaction;
import org.dellroad.stuff.vaadin7.VaadinApplicationListener;
import org.dellroad.stuff.vaadin7.VaadinConfigurable;
import org.jsimpledb.JObject;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.cli.util.CastFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.ApplicationEventMulticaster;
import org.springframework.transaction.annotation.Transactional;

/**
 * Container that contains all database objects of give type (up to a limit of {@link #MAX_OBJECTS}).
 */
@SuppressWarnings("serial")
@VaadinConfigurable(preConstruction = true)
public class ObjectContainer extends JObjectContainer {

    public static final int MAX_OBJECTS = 1000;

    private DataChangeListener dataChangeListener;
    private JObject lowerBound;
    private Query query;

    @Autowired
    @Qualifier("jsimpledbGuiEventMulticaster")
    private ApplicationEventMulticaster eventMulticaster;

    /**
     * Constructor.
     *
     * @param type type restriction, or null for no restriction
     */
    public <T> ObjectContainer(JSimpleDB jdb, Class<T> type) {
        super(jdb, type);
    }

    @RetryTransaction
    @Transactional("jsimpledbGuiTransactionManager")
    protected void doInTransaction(Runnable action) {
        action.run();
    }

    public void reload(Query query) {
        this.query = query;
        super.reload();
    }

    public void reload() {
        if (this.query != null)
            this.reload(this.query);
    }

    @Override
    protected Iterable<? extends JObject> queryForObjects() {
        return Iterables.limit(
          Iterables.transform(this.query.query(this.type), new CastFunction<JObject>(JObject.class)),
          MAX_OBJECTS);
    }

// Connectable

    @Override
    public void connect() {
        super.connect();
        this.dataChangeListener = new DataChangeListener();
        this.dataChangeListener.register();
    }

    @Override
    public void disconnect() {
        if (this.dataChangeListener != null) {
            this.dataChangeListener.unregister();
            this.dataChangeListener = null;
        }
        super.disconnect();
    }

// Query

    /**
     * Callback interface used when loading objects into an {@link ObjectContainer}.
     */
    public interface Query {

        /**
         * Get the objects to load into the {@link ObjectContainer}.
         * At most {@link ObjectContainer#MAX_OBJECTS} objects are shown, and objects that are
         * not instances of the type associated with the container will be filtered out (if {@code type} is not null).
         * A transaction will be open.
         *
         * @param type type restriction, or null for none
         */
        <T> Iterable<?> query(Class<T> type);
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

