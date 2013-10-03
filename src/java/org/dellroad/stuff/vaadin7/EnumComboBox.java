
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin7;

import com.vaadin.shared.ui.combobox.FilteringMode;
import com.vaadin.ui.AbstractSelect.ItemCaptionMode;
import com.vaadin.ui.ComboBox;

/**
 * {@link ComboBox} that chooses an {@link Enum} value.
 *
 * @see EnumContainer
 */
@SuppressWarnings("serial")
public class EnumComboBox extends ComboBox {

    /**
     * Convenience constructor.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><code>
     *  EnumComboBox(new EnumContainer(type), EnumContainer.TO_STRING_PROPERTY.getName(), false);
     *  </code></blockquote>
     * </p>
     *
     * @param enumClass enum type
     */
    public <T extends Enum<T>> EnumComboBox(Class<T> enumClass) {
        this(enumClass, false);
    }

    /**
     * Convenience constructor.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><code>
     *  EnumComboBox(new EnumContainer(type), EnumContainer.TO_STRING_PROPERTY, allowNull);
     *  </code></blockquote>
     * </p>
     *
     * @param enumClass enum type
     * @param allowNull true to allow a null selection, false otherwise
     */
    public <T extends Enum<T>> EnumComboBox(Class<T> enumClass, boolean allowNull) {
        this(enumClass, EnumContainer.TO_STRING_PROPERTY, allowNull);
    }

    /**
     * Convenience constructor.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><code>
     *  EnumComboBox(new EnumContainer(type), displayPropertyName, false);
     *  </code></blockquote>
     * </p>
     *
     * @param enumClass enum type
     * @param displayPropertyName container property to display in the combo box
     */
    public <T extends Enum<T>> EnumComboBox(Class<T> enumClass, String displayPropertyName) {
        this(enumClass, displayPropertyName, false);
    }

    /**
     * Convenience constructor.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><code>
     *  EnumComboBox(new EnumContainer(type), displayPropertyName, allowNull);
     *  </code></blockquote>
     * </p>
     *
     * @param enumClass enum type
     * @param displayPropertyName container property to display in the combo box
     * @param allowNull true to allow a null selection, false otherwise
     */
    public <T extends Enum<T>> EnumComboBox(Class<T> enumClass, String displayPropertyName, boolean allowNull) {
        this(new EnumContainer<T>(enumClass), displayPropertyName, allowNull);
    }

    /**
     * Primary constructor.
     *
     * @param container container data source
     * @param displayPropertyName container property to display in the combo box
     * @param allowNull true to allow a null selection, false otherwise
     */
    public EnumComboBox(EnumContainer<?> container, String displayPropertyName, boolean allowNull) {
        this.setContainerDataSource(container);
        this.setNewItemsAllowed(false);
        this.setTextInputAllowed(false);
        this.setFilteringMode(FilteringMode.OFF);
        this.setItemCaptionMode(ItemCaptionMode.PROPERTY);
        this.setItemCaptionPropertyId(displayPropertyName);

        // Set up whether null selection is allowed
        this.setNullSelectionAllowed(allowNull);
        if (!allowNull)
            this.setValue(this.getContainerDataSource().getItemIds().iterator().next());
    }
}

