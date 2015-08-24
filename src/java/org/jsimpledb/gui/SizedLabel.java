
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.gui;

import com.vaadin.data.Property;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Label;

/**
 * A {@link Label} initialized to have undefined size, so its size shrinks to the content.
 */
@SuppressWarnings("serial")
public class SizedLabel extends Label {

    public SizedLabel() {
        this.setSizeUndefined();
    }

    public SizedLabel(Property<?> dataSource) {
        super(dataSource);
        this.setSizeUndefined();
    }

    public SizedLabel(Property<?> dataSource, ContentMode contentMode) {
        super(dataSource, contentMode);
        this.setSizeUndefined();
    }

    public SizedLabel(String content) {
        super(content);
        this.setSizeUndefined();
    }

    public SizedLabel(String content, ContentMode contentMode) {
        super(content, contentMode);
        this.setSizeUndefined();
    }
}

