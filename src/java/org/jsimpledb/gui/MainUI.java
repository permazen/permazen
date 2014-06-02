
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.ui.Component;

@SuppressWarnings("serial")
public class MainUI extends AbstractUI {

    public static final String URI_PATH = "main";

    @Override
    protected Component buildMainPanel() {
        return new MainPanel();
    }
}

