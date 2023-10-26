
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin.app;

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

import io.permazen.vaadin.SizedLabel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support superclass for Vaadin {@link UI} implementations.
 */
@PreserveOnRefresh
@Push
@SuppressWarnings("serial")
@Theme("jsdb")
public abstract class AbstractUI extends UI {

    private static final float UPPER_BAR_HEIGHT = 44;

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final VerticalLayout rootLayout = new VerticalLayout();

// Vaadin lifecycle

    @Override
    public void init(VaadinRequest request) {
        this.getPage().setTitle(this.getTitle());
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

    protected String getTitle() {
        return "Permazen";
    }

    protected Component getTopRightLabel() {
        return null;
    }

    protected Component buildRootUpperBar() {

        // Logo
        final Link logo = new Link(null, new ExternalResource(MainUI.URI_PATH));
        logo.setIcon(new ThemeResource("img/permazen-logo-48x48.png"));
        final HorizontalLayout logoLayout = new HorizontalLayout();
        logoLayout.addStyleName("jsdb-upper-bar-company-logo-layout");
        logoLayout.setWidth(48, Sizeable.Unit.PIXELS);
        logoLayout.addComponent(logo);
        logoLayout.setComponentAlignment(logo, Alignment.BOTTOM_LEFT);

        // Title
        final SizedLabel titleLabel = new SizedLabel(this.getTitle());
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
        final Component topRightLabel = this.getTopRightLabel();
        if (topRightLabel != null) {
            layout.addComponent(topRightLabel);
            layout.setComponentAlignment(topRightLabel, Alignment.BOTTOM_RIGHT);
        }
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
        footerLayout.addComponent(new SizedLabel("Permazen Viewer"));
        layout.addComponent(footerLayout);

        // Done
        return layout;
    }

    protected abstract Component buildMainPanel();
}
