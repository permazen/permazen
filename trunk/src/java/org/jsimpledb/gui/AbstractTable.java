
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.data.Container;
import com.vaadin.ui.Table;

import java.util.ArrayList;

import org.dellroad.stuff.vaadin7.Connectable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support superclass for {@link Table}s that are based on {@link Connectable} {@link Container}s.
 *
 * @param <C> container type
 */
@SuppressWarnings("serial")
public abstract class AbstractTable<C extends Container & Connectable> extends Table {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private C container;

    private ArrayList<String> columnIds;

    protected AbstractTable() {
    }

    protected AbstractTable(String caption) {
        super(caption);
    }

    protected void addColumn(String property, String name, int width, Table.Align alignment) {
        if (property == null)
            throw new IllegalArgumentException("null property");
        if (name == null)
            throw new IllegalArgumentException("null name");
        this.setColumnHeader(property, name);
        this.setColumnWidth(property, width);
        if (alignment != null)
            this.setColumnAlignment(property, alignment);
        this.columnIds.add(property);
    }

    public C getContainer() {
        return this.container;
    }

    protected abstract C buildContainer();

    protected abstract void configureColumns();

// Vaadin lifecycle

    @Override
    public void attach() {
        super.attach();

        // Connect container
        this.container = this.buildContainer();
        this.container.connect();
        this.setContainerDataSource(this.container);

        // Setup columns
        this.columnIds = new ArrayList<String>();
        this.configureColumns();
        this.setVisibleColumns(this.columnIds.toArray());
    }

    @Override
    public void detach() {
        if (this.container != null) {
            this.container.disconnect();
            this.container = null;
        }
        super.detach();
    }
}

