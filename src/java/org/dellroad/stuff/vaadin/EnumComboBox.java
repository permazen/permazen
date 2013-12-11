
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.vaadin;

import com.vaadin.ui.AbstractSelect;
import com.vaadin.ui.ComboBox;

/**
 * {@link ComboBox} that chooses an {@link Enum} value.
 *
 * @see EnumContainer
 */
@SuppressWarnings("serial")
public class EnumComboBox extends ComboBox {

    /**
     * Default constructor.
     *
     * <p>
     * Caller must separately invoke {@link #setEnumDataSource}.
     * </p>
     */
    public EnumComboBox() {
        this(null);
    }

    /**
     * Convenience constructor.
     *
     * <p>
     * Equivalent to:
     *  <blockquote><code>
     *  EnumComboBox(enumClass, EnumContainer.TO_STRING_PROPERTY.getName(), false);
     *  </code></blockquote>
     * </p>
     *
     * @param enumClass enum type, or null to leave data source unset
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
     *  EnumComboBox(enumClass, EnumContainer.TO_STRING_PROPERTY, allowNull);
     *  </code></blockquote>
     * </p>
     *
     * @param enumClass enum type, or null to leave data source unset
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
     *  EnumComboBox(enumClass, displayPropertyName, false);
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
     *  EnumComboBox(enumClass != null ? new EnumContainer&lt;T&gt;(enumClass) : null, displayPropertyName, allowNull);
     *  </code></blockquote>
     * </p>
     *
     * @param enumClass enum type, or null to leave data source unset
     * @param displayPropertyName container property to display in the combo box
     * @param allowNull true to allow a null selection, false otherwise
     */
    public <T extends Enum<T>> EnumComboBox(Class<T> enumClass, String displayPropertyName, boolean allowNull) {
        this(enumClass != null ? new EnumContainer<T>(enumClass) : null, displayPropertyName, allowNull);
    }

    /**
     * Primary constructor.
     *
     * <p>
     * This instance is configured for item caption {@link AbstractSelect#ITEM_CAPTION_MODE_PROPERTY} mode, with
     * {@code displayPropertyName} as the {@linkplain #setItemCaptionPropertyId item caption property}.
     * </p>
     *
     * @param container container data source, or null to leave data source unset
     * @param displayPropertyName container property to display in the combo box
     * @param allowNull true to allow a null selection, false otherwise
     */
    public EnumComboBox(EnumContainer<?> container, String displayPropertyName, boolean allowNull) {
        if (container != null)
            this.setContainerDataSource(container);
        this.setNewItemsAllowed(false);
        this.setTextInputAllowed(false);
        this.setFilteringMode(AbstractSelect.Filtering.FILTERINGMODE_OFF);
        this.setItemCaptionMode(AbstractSelect.ITEM_CAPTION_MODE_PROPERTY);
        this.setItemCaptionPropertyId(displayPropertyName);

        // Set up whether null selection is allowed
        this.setNullSelectionAllowed(allowNull);
        if (!allowNull && !this.getContainerDataSource().getItemIds().isEmpty())
            this.setValue(this.getContainerDataSource().getItemIds().iterator().next());
    }

    /**
     * Set the {@link Enum} type whose instances serve as this instance's data source.
     *
     * @throws IllegalArgumentException if {@code enumClass} is null
     */
    public <T extends Enum<T>> void setEnumDataSource(Class<T> enumClass) {
        this.setContainerDataSource(new EnumContainer<T>(enumClass));
        if (!this.isNullSelectionAllowed() && !this.getContainerDataSource().getItemIds().isEmpty())
            this.setValue(this.getContainerDataSource().getItemIds().iterator().next());
    }
}

