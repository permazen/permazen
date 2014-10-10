
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.gui;

import com.google.common.reflect.TypeToken;
import com.vaadin.data.Property;
import com.vaadin.server.Sizeable;
import com.vaadin.shared.ui.combobox.FilteringMode;
import com.vaadin.ui.AbstractSelect;
import com.vaadin.ui.Button;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.HorizontalSplitPanel;
import com.vaadin.ui.TextArea;

import java.util.HashSet;

import org.jsimpledb.JClass;
import org.jsimpledb.JSimpleDB;
import org.jsimpledb.core.ObjId;
import org.jsimpledb.core.UnknownTypeException;
import org.jsimpledb.parse.ParseSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains widgets that allow choosing an object reference. Supports searching and filtering.
 */
@SuppressWarnings({ "serial", "deprecation" })
public class ObjectChooser implements Property.ValueChangeNotifier {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Button showButton = new Button("Show", new Button.ClickListener() {
        @Override
        public void buttonClick(Button.ClickEvent event) {
            ObjectChooser.this.showButtonClicked();
        }
    });

    private final JSimpleDB jdb;
    private final ParseSession session;
    private final boolean showFields;
    private final TypeContainer typeContainer;
    private final ObjectContainer objectContainer;
    private final HashSet<Property.ValueChangeListener> listeners = new HashSet<>();

    private final HorizontalSplitPanel splitPanel = new HorizontalSplitPanel();
    private final FormLayout showForm = new FormLayout();
    private final TypeTable typeTable;
    private final TextArea exprField = new TextArea();
    private final ComboBox sortComboBox = new ComboBox();

    private ObjectTable objectTable;
    private SortKeyContainer sortKeyContainer;

    /**
     * Constructor.
     *
     * @param jdb database
     * @param session session for evaluating expressions
     * @param type type restriction, or null for none
     * @param showFields true to show all object fields, false for just reference label
     */
    public ObjectChooser(JSimpleDB jdb, ParseSession session, Class<?> type, boolean showFields) {
        if (jdb == null)
            throw new IllegalArgumentException("null jdb");
        if (session == null)
            throw new IllegalArgumentException("null session");
        this.jdb = jdb;
        this.session = session;
        this.showFields = showFields;
        this.typeContainer = new TypeContainer(this.jdb, type);
        this.typeTable = new TypeTable(this.typeContainer);
        this.objectContainer = new ObjectContainer(this.jdb, type, this.session);

        // Build object panel
        this.splitPanel.setWidth("100%");
        this.splitPanel.setHeight(300, Sizeable.Unit.PIXELS);
        this.splitPanel.setFirstComponent(this.typeTable);
        this.splitPanel.setSecondComponent(new SizedLabel(" "));
        this.splitPanel.setSplitPosition(20);

        // Build show form
        this.showForm.setMargin(false);
        this.showForm.setWidth("100%");

        // Add "Show all by" combo box
        final HorizontalLayout sortLayout = new HorizontalLayout();
        sortLayout.setCaption("Show all by:");
        sortLayout.setSpacing(true);
        sortLayout.setMargin(false);
        this.sortComboBox.setNullSelectionAllowed(false);
        this.sortComboBox.setNewItemsAllowed(false);
        this.sortComboBox.setTextInputAllowed(false);
        this.sortComboBox.setFilteringMode(FilteringMode.OFF);
        this.sortComboBox.setItemCaptionMode(AbstractSelect.ItemCaptionMode.PROPERTY);
        this.sortComboBox.setItemCaptionPropertyId(SortKeyContainer.DESCRIPTION_PROPERTY);
        this.sortComboBox.setImmediate(true);
        sortLayout.addComponent(this.sortComboBox);
        this.showForm.addComponent(sortLayout);

        // Add show/expression field
        this.exprField.setCaption("Expression:");
        this.exprField.setRows(4);
        this.exprField.setWidth("100%");
        this.exprField.addStyleName("jsdb-fixed-width");
        this.showForm.addComponent(this.exprField);
        this.showForm.addComponent(this.showButton);

        // Listen to type selections
        this.typeTable.addValueChangeListener(new Property.ValueChangeListener() {
            @Override
            public void valueChange(Property.ValueChangeEvent event) {
                ObjectChooser.this.selectType((TypeToken<?>)event.getProperty().getValue(), false);
            }
        });

        // Listen to "Show all by" selection
        this.sortComboBox.addValueChangeListener(new Property.ValueChangeListener() {
            @Override
            public void valueChange(Property.ValueChangeEvent event) {
                ObjectChooser.this.selectSort((SortKeyContainer.SortKey)event.getProperty().getValue());
            }
        });

        // Populate table
        this.selectType(TypeToken.of(type != null ? type : Object.class), true);
    }

    /**
     * Get the object container for this instance.
     */
    public ObjectContainer getObjectContainer() {
        return this.objectContainer;
    }

    /**
     * Get the current type restriction, if any.
     */
    public Class<?> getType() {
        return this.objectContainer.getType();
    }

    /**
     * Get the type table.
     */
    public TypeTable getTypeTable() {
        return this.typeTable;
    }

    /**
     * Get the component containing the type chooser and object panel.
     */
    public HorizontalSplitPanel getObjectPanel() {
        return this.splitPanel;
    }

    /**
     * Get the currently selected object, if any.
     */
    public ObjId getObjId() {
        return this.objectTable != null ? (ObjId)this.objectTable.getValue() : null;
    }

    /**
     * Get the form with the sort chooser and expression text area.
     */
    public FormLayout getShowForm() {
        return this.showForm;
    }

    /**
     * Get the {@link JClass} corresponding to the currently selected type, if any.
     */
    public JClass<?> getJClass() {
        final Class<?> type = this.objectContainer.getType();
        if (type == null)
            return null;
        try {
            return this.jdb.getJClass(type);
        } catch (UnknownTypeException e) {
            return null;
        }
    }

// Property.ValueChangeNotifier

    @Override
    public void addValueChangeListener(Property.ValueChangeListener listener) {
        if (listener == null)
            throw new IllegalArgumentException("null listener");
        this.listeners.add(listener);
        if (this.objectTable != null)
            this.objectTable.addValueChangeListener(listener);
    }

    @Override
    public void removeValueChangeListener(Property.ValueChangeListener listener) {
        if (listener == null)
            return;
        this.listeners.remove(listener);
        if (this.objectTable != null)
            this.objectTable.removeValueChangeListener(listener);
    }

    @Override
    public void addListener(Property.ValueChangeListener listener) {
        this.addValueChangeListener(listener);
    }

    @Override
    public void removeListener(Property.ValueChangeListener listener) {
        this.removeValueChangeListener(listener);
    }

// GUI Updates

    // Invoked when a type is clicked on
    private void selectType(TypeToken<?> typeToken, boolean force) {

        // Anything to do?
        if (typeToken == null)
            return;
        if (!this.setNewType(!typeToken.equals(TypeToken.of(Object.class)) ? typeToken.getRawType() : null, force))
            return;

        // Rebuild the sort combobox
        final JClass<?> jclass = this.getJClass();
        this.sortKeyContainer = jclass != null ?
          new SortKeyContainer(this.jdb, jclass) : new SortKeyContainer(this.jdb, this.objectContainer.getType());
        this.sortComboBox.setContainerDataSource(this.sortKeyContainer);

        // Default to sorting by object ID
        this.sortComboBox.setValue(this.sortKeyContainer.new ObjectIdSortKey());
        this.selectSort((SortKeyContainer.SortKey)this.sortComboBox.getValue());
        this.showObjects();
    }

    // Invoked when a sort choice is made
    private void selectSort(SortKeyContainer.SortKey sortKey) {
        if (sortKey == null)
            return;
        this.exprField.setValue(sortKey.getExpression(null, false));        // TODO: starting point and sort order
        this.showObjects();
    }

    // Invoked when "Show" button is clicked
    private void showButtonClicked() {
        this.setNewType(null, false);
        this.typeTable.setValue(null);
        this.showObjects();
    }

    private void showObjects() {
        this.objectContainer.setContentExpression(this.exprField.getValue());
    }

    private boolean setNewType(Class<?> type, boolean force) {
        final Class<?> currentType = this.objectContainer.getType();
        if (!force && (currentType != null ? currentType.equals(type) : type == null))
            return false;
        if (this.objectTable != null) {
            for (Property.ValueChangeListener listener : this.listeners)
                this.objectTable.removeValueChangeListener(listener);
            this.splitPanel.removeComponent(this.objectTable);
        }
        this.objectContainer.setType(type);
        this.objectTable = new ObjectTable(this.jdb, this.objectContainer, this.session, this.showFields);
        for (Property.ValueChangeListener listener : this.listeners)
            this.objectTable.addValueChangeListener(listener);
        this.splitPanel.setSecondComponent(this.objectTable);
        this.objectTable.setValue(null);
        final Property.ValueChangeEvent event = new Property.ValueChangeEvent() {
            @Override
            public Property getProperty() {
                return ObjectChooser.this.objectTable;
            }
        };
        for (Property.ValueChangeListener listener : new HashSet<Property.ValueChangeListener>(this.listeners)) {
            try {
                listener.valueChange(event);
            } catch (Exception e) {
                this.log.error("exception thrown by value change listener", e);
            }
        }
        return true;
    }
}

