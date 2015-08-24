
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.gui;

import com.vaadin.server.UIClassSelectionEvent;
import com.vaadin.server.UIProvider;
import com.vaadin.ui.UI;

/**
 * {@link UIProvider} for this GUI.
 */
@SuppressWarnings("serial")
public class GUIUIProvider extends UIProvider {

    @Override
    public Class<? extends UI> getUIClass(UIClassSelectionEvent event) {

        // Get requested URI path
        String pathInfo = event.getRequest().getPathInfo();
        if (pathInfo != null)
            pathInfo = pathInfo.substring(1);

        // Look for well-known patterns... TODO

        // Default to main root
        return MainUI.class;
    }
}

