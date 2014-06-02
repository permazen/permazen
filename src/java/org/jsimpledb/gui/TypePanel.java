
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.ui.VerticalLayout;

@SuppressWarnings("serial")
public class TypePanel extends VerticalLayout {

    private final MainPanel mainPanel;

    /**
     * Constructor.
     *
     * @param type type of the Java objects that back each item in the container
     */
    public TypePanel(MainPanel mainPanel) {
        this.mainPanel = mainPanel;
        this.setMargin(true);
        this.setSpacing(true);
        this.setHeight("100%");
        this.addComponent(new TypeTable(this.mainPanel));
    }
}

