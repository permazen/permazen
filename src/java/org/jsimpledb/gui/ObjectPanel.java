
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.ui.VerticalLayout;

import org.jsimpledb.JClass;

@SuppressWarnings("serial")
public class ObjectPanel extends VerticalLayout {

    private final MainPanel mainPanel;
    private final JClass<?> jclass;

    /**
     * Constructor.
     *
     * @param type type of the Java objects that back each item in the container
     */
    public ObjectPanel(MainPanel mainPanel, JClass<?> jclass) {
        this.mainPanel = mainPanel;
        this.jclass = jclass;
        this.setMargin(true);
        this.setSpacing(true);
        this.setHeight("100%");
        this.addComponent(new ObjectTable(this.mainPanel, this.jclass));
    }
}

