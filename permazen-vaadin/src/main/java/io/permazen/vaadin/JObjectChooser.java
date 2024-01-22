
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin;

import com.google.common.base.Preconditions;
import com.vaadin.data.Property;
import com.vaadin.server.Sizeable;
import com.vaadin.shared.ui.combobox.FilteringMode;
import com.vaadin.ui.AbstractSelect;
import com.vaadin.ui.Button;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.ComboBox;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.HorizontalSplitPanel;
import com.vaadin.ui.TextArea;

import io.permazen.PermazenClass;
import io.permazen.PermazenObject;
import io.permazen.Permazen;
import io.permazen.core.ObjId;
import io.permazen.parse.ParseSession;

import java.util.Collections;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains widgets that allow choosing an object reference. Supports searching via Java expression and filtering.
 */
@SuppressWarnings({ "serial", "deprecation" })
public class JObjectChooser implements Property.ValueChangeNotifier {

    protected final Logger log = LoggerFactory.getLogger(this.getClass());

    private final Button showButton = new Button("Show", e -> this.showButtonClicked());
    private final CheckBox reverseCheckBox = new CheckBox("Reverse sort");

    private final Permazen pdb;
    private final ParseSession session;
    private final boolean showFields;
    private final TypeContainer typeContainer;
    private final ExprQueryJObjectContainer objectContainer;
    private final HashSet<Property.ValueChangeListener> listeners = new HashSet<>();

    private final HorizontalSplitPanel splitPanel = new HorizontalSplitPanel();
    private final FormLayout showForm = new FormLayout();
    private final TypeTable typeTable;
    private final TextArea exprField = new TextArea();
    private final ComboBox sortComboBox = new ComboBox();

    private JObjectTable objectTable;
    private SortKeyContainer sortKeyContainer;

    /**
     * Constructor.
     *
     * @param session session for evaluating expressions
     * @param type type restriction, or null for none
     * @param showFields true to show all object fields, false for just reference label
     */
    public JObjectChooser(ParseSession session, Class<?> type, boolean showFields) {
        Preconditions.checkArgument(session != null, "null session");
        this.pdb = session.getPermazen();
        this.session = session;
        this.showFields = showFields;
        this.typeContainer = new TypeContainer(this.pdb, type);
        this.typeTable = new TypeTable(this.typeContainer);
        this.objectContainer = new ExprQueryJObjectContainer(this.session, this.typeContainer.getRootType());

        // Build object panel
        this.splitPanel.setWidth("100%");
        this.splitPanel.setHeight(300, Sizeable.Unit.PIXELS);
        this.splitPanel.setFirstComponent(this.typeTable);
        this.splitPanel.setSecondComponent(new SizedLabel(" "));
        this.splitPanel.setSplitPosition(20);

        // Build show form
        this.showForm.setMargin(false);
        this.showForm.setWidth("100%");

        // Add "Sort by" combo box and "Reverse sort" check box
        final HorizontalLayout sortLayout = new HorizontalLayout();
        sortLayout.setCaption("Sort by:");
        sortLayout.setSpacing(true);
        sortLayout.setMargin(false);
        this.sortComboBox.setNullSelectionAllowed(false);
        this.sortComboBox.setNewItemsAllowed(false);
        this.sortComboBox.setTextInputAllowed(false);
        this.sortComboBox.setFilteringMode(FilteringMode.OFF);
        this.sortComboBox.setItemCaptionMode(AbstractSelect.ItemCaptionMode.PROPERTY);
        this.sortComboBox.setItemCaptionPropertyId(SortKeyContainer.DESCRIPTION_PROPERTY);
        this.sortComboBox.setImmediate(true);
        this.reverseCheckBox.setImmediate(true);
        sortLayout.addComponent(this.sortComboBox);
        sortLayout.addComponent(this.reverseCheckBox);
        this.showForm.addComponent(sortLayout);

        // Add show/expression field
        this.exprField.setCaption("Expression:");
        this.exprField.setRows(6);
        this.exprField.setWidth("100%");
        this.exprField.addStyleName("jsdb-fixed-width");
        this.showForm.addComponent(this.exprField);
        this.showForm.addComponent(this.showButton);

        // Listen to type selections
        this.typeTable.addValueChangeListener(e -> this.selectType((Class<?>)e.getProperty().getValue(), false));

        // Listen to "Sort by" and "Reverse sort" selections
        final Property.ValueChangeListener sortListener = e -> this.selectSort();
        this.sortComboBox.addValueChangeListener(sortListener);
        this.reverseCheckBox.addValueChangeListener(sortListener);

        // Populate table
        this.selectType(type != null ? type : this.typeContainer.getRootType(), true);
    }

    /**
     * Get the {@link ParseSession} used by this instance.
     *
     * @return associated {@link ParseSession}
     */
    public ParseSession getParseSession() {
        return this.session;
    }

    /**
     * Get the object container for this instance.
     *
     * @return associated object container
     */
    public ExprQueryJObjectContainer getJObjectContainer() {
        return this.objectContainer;
    }

    /**
     * Get the current type restriction, if any.
     *
     * @return current type restriction
     */
    public Class<?> getType() {
        return this.objectContainer.getType();
    }

    /**
     * Get the type table.
     *
     * @return type selection table
     */
    public TypeTable getTypeTable() {
        return this.typeTable;
    }

    /**
     * Get the component containing the type chooser and object panel.
     *
     * @return object panel
     */
    public HorizontalSplitPanel getObjectPanel() {
        return this.splitPanel;
    }

    /**
     * Get the currently selected object, if any.
     *
     * @return selected object ID, or null if none
     */
    public ObjId getObjId() {
        return this.objectTable != null ? (ObjId)this.objectTable.getValue() : null;
    }

    /**
     * Get the form with the sort chooser and expression text area.
     *
     * @return expression form
     */
    public FormLayout getShowForm() {
        return this.showForm;
    }

    /**
     * Get the {@link PermazenClass} corresponding to the currently selected type, if any.
     *
     * @return selected type object type
     */
    public PermazenClass<?> getPermazenClass() {
        final Class<?> type = this.objectContainer.getType();
        if (type == null)
            return null;
        try {
            return this.pdb.getPermazenClass(type);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

// Property.ValueChangeNotifier

    @Override
    public void addValueChangeListener(Property.ValueChangeListener listener) {
        Preconditions.checkArgument(listener != null, "null listener");
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
    private void selectType(Class<?> type, boolean force) {

        // Anything to do?
        if (type == null)
            return;
        if (!this.setNewType(!type.equals(Object.class) ? type : null, force))
            return;

        // Get previous sort key
        final SortKeyContainer.SortKey previousSort = (SortKeyContainer.SortKey)this.sortComboBox.getValue();

        // Rebuild the sort combobox
        final PermazenClass<?> jclass = this.getPermazenClass();
        this.sortKeyContainer = jclass != null ?
          new SortKeyContainer(this.pdb, jclass) : new SortKeyContainer(this.pdb, this.objectContainer.getType());
        this.sortComboBox.setContainerDataSource(this.sortKeyContainer);

        // Try to restore previous sort, otherwise default to sorting by object ID
        SortKeyContainer.SortKey newSort = this.sortKeyContainer.getJavaObject(previousSort);
        if (newSort == null)
            newSort = this.sortKeyContainer.new ObjectIdSortKey();
        this.sortComboBox.setValue(newSort);
        this.selectSort();
        this.showObjects();
    }

    // Update sort based on current selection
    private void selectSort() {
        this.selectSort((SortKeyContainer.SortKey)this.sortComboBox.getValue(), this.reverseCheckBox.getValue());
    }

    // Invoked when a sort choice is made
    private void selectSort(SortKeyContainer.SortKey sortKey, boolean reverse) {
        if (sortKey == null)
            return;
        this.exprField.setValue(sortKey.getExpression(this.session, null, reverse));          // TODO: starting point
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
        this.objectContainer.load(Collections.<PermazenObject>emptySet());
        this.objectTable = new JObjectTable(this.pdb, this.objectContainer, this.session, this.showFields);
        for (Property.ValueChangeListener listener : this.listeners)
            this.objectTable.addValueChangeListener(listener);
        this.splitPanel.setSecondComponent(this.objectTable);
        this.objectTable.setValue(null);
        final Property.ValueChangeEvent event = new Property.ValueChangeEvent() {
            @Override
            public Property<?> getProperty() {
                return JObjectChooser.this.objectTable;
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
