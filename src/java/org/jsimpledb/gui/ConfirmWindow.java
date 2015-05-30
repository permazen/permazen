
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.gui;

import com.vaadin.server.Sizeable;
import com.vaadin.ui.Button;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;

/**
 * A confirmation window. Content and action on confirmation are supplied by the subclass.
 */
@SuppressWarnings("serial")
public abstract class ConfirmWindow extends Window {

    protected final Button okButton;
    protected final Button cancelButton;
    protected final UI ui;

    private boolean populated;

    protected ConfirmWindow(UI ui, String title) {
        this(ui, title, "OK", "Cancel");
    }

    protected ConfirmWindow(UI ui, String title, String okLabel) {
        this(ui, title, okLabel, "Cancel");
    }

    protected ConfirmWindow(UI ui, String title, String okLabel, String cancelLabel) {
        super(title);
        this.ui = ui;

        // Initialize self (defaults)
        this.setWidth(450, Sizeable.Unit.PIXELS);
        this.setHeight(300, Sizeable.Unit.PIXELS);
        this.setClosable(true);
        this.setModal(true);

        // Create buttons
        this.okButton = okLabel != null ? new Button(okLabel, new Button.ClickListener() {
            @Override
            public void buttonClick(Button.ClickEvent event) {
                if (ConfirmWindow.this.execute())
                    ConfirmWindow.this.closeWindow();
            }
        }) : null;
        this.cancelButton = cancelLabel != null ? new Button(cancelLabel, new Button.ClickListener() {
            @Override
            public void buttonClick(Button.ClickEvent event) {
                ConfirmWindow.this.closeWindow();
            }
        }) : null;
    }

    @Override
    public void attach() {

        // Already done?
        if (this.populated)
            return;
        this.populated = true;

        // Get layout
        VerticalLayout layout = new VerticalLayout();
        layout.setMargin(true);
        layout.setSpacing(true);
        layout.setSizeFull();
        this.setContent(layout);

        // Add subclass content
        this.addContent(layout);

        // Add buttons
        Label spacer = new Label();
        layout.addComponent(spacer);
        layout.setExpandRatio(spacer, 1);
        HorizontalLayout buttonLayout = new HorizontalLayout();
        buttonLayout.setSpacing(true);
        if (this.okButton != null)
            buttonLayout.addComponent(this.okButton);
        if (this.cancelButton != null)
            buttonLayout.addComponent(this.cancelButton);
        layout.addComponent(buttonLayout);
    }

    /**
     * Show this window.
     */
    public void show() {
        this.ui.addWindow(this);
    }

    /**
     * Close this window.
     */
    public void closeWindow() {
        this.ui.removeWindow(this);
    }

    /**
     * Add content to the confirmation window's main layout.
     *
     * @param layout new content
     */
    protected abstract void addContent(VerticalLayout layout);

    /**
     * Execute some action after "OK" confirmation.
     * This window will not yet have been removed.
     *
     * @return true if operation was successful and window should be removed
     */
    protected abstract boolean execute();
}

