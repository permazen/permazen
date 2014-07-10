
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.vaadin.annotations.PreserveOnRefresh;
import com.vaadin.annotations.Push;
import com.vaadin.annotations.Theme;
import com.vaadin.server.ExternalResource;
import com.vaadin.server.Sizeable;
import com.vaadin.server.ThemeResource;
import com.vaadin.server.VaadinRequest;
import com.vaadin.shared.ui.MarginInfo;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Component;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Link;
import com.vaadin.ui.UI;
import com.vaadin.ui.VerticalLayout;

import org.dellroad.stuff.vaadin7.VaadinConfigurable;
import org.jsimpledb.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

/**
 * Superclass of the various {@link UI}s that constitute the GUI.
 */
@PreserveOnRefresh
@Push
@SuppressWarnings("serial")
@Theme("jsdb")
@VaadinConfigurable
public abstract class AbstractUI extends UI {

    private static final float UPPER_BAR_HEIGHT = 44;

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final VerticalLayout rootLayout = new VerticalLayout();

    @Autowired
    @Qualifier("jsimpledbGuiMain")
    private Main main;

// Constructor

    protected AbstractUI() {
        this(null);
    }

    protected AbstractUI(String title) {
        this.getPage().setTitle("JSimpleDB GUI" + (title != null ? " " + title : ""));
    }

// Vaadin lifecycle

    @Override
    public void init(VaadinRequest request) {
        this.setContent(this.rootLayout);
        this.rootLayout.setSpacing(true);
        this.rootLayout.setSizeFull();
        this.rootLayout.setMargin(new MarginInfo(false, true, true, true));
        this.rootLayout.addComponent(this.buildRootUpperBar());
        this.rootLayout.addComponent(new HorizontalLine(3));
        final Component lowerPanel = this.buildLowerPanel();
        this.rootLayout.addComponent(lowerPanel);
        this.rootLayout.setExpandRatio(lowerPanel, 1.0f);
        this.rootLayout.setComponentAlignment(lowerPanel, Alignment.MIDDLE_CENTER);
    }

// Layout construction

    protected Component buildRootUpperBar() {

        // Logo
        final Link logo = new Link(null, new ExternalResource("main"));
        logo.setIcon(new ThemeResource("img/jsimpledb-logo-48x48.png"));
        final HorizontalLayout logoLayout = new HorizontalLayout();
        logoLayout.addStyleName("jsdb-upper-bar-company-logo-layout");
        logoLayout.setWidth(48, Sizeable.Unit.PIXELS);
        logoLayout.addComponent(logo);
        logoLayout.setComponentAlignment(logo, Alignment.BOTTOM_LEFT);

        // Title
        final SizedLabel titleLabel = new SizedLabel("JSimpleDB Viewer");
        titleLabel.addStyleName("jsdb-title");

        // Sequence parts
        final HorizontalLayout layout = new HorizontalLayout();
        layout.setSpacing(true);
        layout.setWidth("100%");
        layout.setHeight(UPPER_BAR_HEIGHT, Sizeable.Unit.PIXELS);
        layout.addComponent(logoLayout);
        layout.setComponentAlignment(logoLayout, Alignment.BOTTOM_LEFT);
        layout.addComponent(titleLabel);
        layout.setExpandRatio(titleLabel, 1.0f);
        layout.setComponentAlignment(titleLabel, Alignment.BOTTOM_CENTER);
        final Label versionLabel = new SizedLabel("Schema Version " + this.main.getSchemaVersion());
        layout.addComponent(versionLabel);
        layout.setComponentAlignment(versionLabel, Alignment.BOTTOM_RIGHT);
        return layout;
    }

    protected Component buildLowerPanel() {

        // Add main panel
        final VerticalLayout layout = new VerticalLayout();
        layout.setSizeFull();
        layout.setMargin(false);
        layout.setSpacing(true);
        final Component mainPanel = this.buildMainPanel();
        mainPanel.setSizeFull();
        layout.addComponent(mainPanel);
        layout.setComponentAlignment(mainPanel, Alignment.TOP_CENTER);
        layout.setExpandRatio(mainPanel, 1);

        // Add footer containing links (admin only) and version label
        final HorizontalLayout footerLayout = new HorizontalLayout();
        footerLayout.setWidth("100%");
        footerLayout.setSpacing(true);
        final Label spacer = new Label();
        footerLayout.addComponent(spacer);
        footerLayout.setExpandRatio(spacer, 1);
        footerLayout.addComponent(new SizedLabel("JSimpleDB Version " + Version.JSIMPLEDB_VERSION));
        layout.addComponent(footerLayout);

        // Done
        return layout;
    }

    protected abstract Component buildMainPanel();
}

