
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.ui.Component;
import com.vaadin.ui.TabSheet;
import com.vaadin.ui.VerticalLayout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main GUI panel containing the various tabs.
 */
@SuppressWarnings("serial")
@org.dellroad.stuff.vaadin7.VaadinConfigurable
public class MainPanel extends VerticalLayout {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final TabSheet tabSheet = new TabSheet();

    public MainPanel() {
        this.setHeight("100%");
        this.tabSheet.setHeight("100%");
        final TabSheet.Tab firstTab = this.tabSheet.addTab(new TypePanel(this), "Types");
        firstTab.setClosable(false);
        this.addComponent(this.tabSheet);
    }

    public void addTab(Component component, String name) {
        final TabSheet.Tab tab = this.tabSheet.addTab(component, name);
        tab.setClosable(true);
        this.tabSheet.setSelectedTab(tab);
    }
}

