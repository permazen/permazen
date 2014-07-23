
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.ui.Button;
import com.vaadin.ui.themes.Reindeer;

@SuppressWarnings("serial")
public class SmallButton extends Button {

    public SmallButton() {
        this.setStyleName(Reindeer.BUTTON_SMALL);
    }

    public SmallButton(String caption) {
        super(caption);
        this.setStyleName(Reindeer.BUTTON_SMALL);
    }

    public SmallButton(String caption, Button.ClickListener listener) {
        super(caption, listener);
        this.setStyleName(Reindeer.BUTTON_SMALL);
    }
}

