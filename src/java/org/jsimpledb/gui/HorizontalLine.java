
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.server.Sizeable;
import com.vaadin.ui.Panel;

@SuppressWarnings("serial")
public class HorizontalLine extends Panel {

    public HorizontalLine(float height) {
        this.addStyleName("jsdb-separator");
        this.setWidth("100%");
        this.setHeight(height, Sizeable.Unit.PIXELS);
    }
}

