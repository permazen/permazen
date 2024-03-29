
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.vaadin.app;

import com.google.common.base.Preconditions;
import com.vaadin.shared.ui.datefield.Resolution;
import com.vaadin.ui.CheckBox;
import com.vaadin.ui.Field;
import com.vaadin.ui.PopupDateField;
import com.vaadin.ui.TextField;

import io.permazen.PermazenReferenceField;
import io.permazen.PermazenSimpleField;
import io.permazen.PermazenTransaction;
import io.permazen.encoding.Encoding;
import io.permazen.parse.ParseSession;
import io.permazen.vaadin.NullableField;
import io.permazen.vaadin.SimpleFieldConverter;

import java.util.Date;

import org.dellroad.stuff.vaadin7.EnumComboBox;

/**
 * Builds {@link Field}s for editing {@link PermazenSimpleField} values.
 */
@SuppressWarnings("serial")
public class SimpleFieldFieldBuilder {

    private final PermazenTransaction jtx;
    private final PermazenSimpleField jfield;
    private final ParseSession session;
    private final boolean allowNull;

    /**
     * Constructor.
     *
     * @param jtx target transaction used by {@link io.permazen.vaadin.JObjectChooser}
     * @param jfield the database {@link PermazenSimpleField} for which to build a Vaadin {@link Field}
     * @param session session used by {@link io.permazen.vaadin.JObjectChooser}
     * @param allowNull whether null values are allowed
     */
    public SimpleFieldFieldBuilder(PermazenTransaction jtx, PermazenSimpleField jfield, ParseSession session, boolean allowNull) {
        Preconditions.checkArgument(jtx != null, "null jtx");
        Preconditions.checkArgument(jfield != null, "null jfield");
        Preconditions.checkArgument(session != null, "null session");
        this.jtx = jtx;
        this.jfield = jfield;
        this.session = session;
        this.allowNull = allowNull && !jfield.getTypeToken().isPrimitive();
    }

    /**
     * Build a {@link Field} appropriate for the configured {@link PermazenSimpleField}.
     *
     * @return Vaadin {@link Field} for editing the field value
     */
    public Field<?> buildField() {

        // Get property type
        final Class<?> propertyType = jfield.getTypeToken().getRawType();

        // Build an appropriate field
        Field<?> field;
        if (jfield instanceof PermazenReferenceField)                                          // use object choosers for references
            field = new ReferenceFieldField(this.jtx, this.session, jfield.getName(), jfield.getTypeToken().getRawType());
        else if (Enum.class.isAssignableFrom(propertyType)) {                           // use ComboBox for Enum's
            final EnumComboBox comboBox = this.createEnumComboBox(propertyType.asSubclass(Enum.class));
            comboBox.setInputPrompt("Null");
            field = comboBox;
        } else if (propertyType.isAssignableFrom(Date.class)) {                         // use DatePicker for dates
            final PopupDateField dateField = new PopupDateField();
            dateField.setResolution(Resolution.SECOND);
            field = dateField;
        } else if (propertyType == boolean.class || propertyType == Boolean.class)      // use CheckBox for booleans
            field = new CheckBox();
        else {                                                                          // use text field for all others
            final TextField textField = new TextField();
            textField.setWidth("100%");
            textField.setNullRepresentation("");
            textField.setNullSettingAllowed(false);
            final Encoding<?> encoding = jfield.getEncoding();
            textField.setConverter(this.buildSimpleFieldConverter(encoding));
            field = textField;
        }

        // Add a "Null" button if field can be null
        if (this.allowNull)
            field = this.addNullButton(field);

        // Done
        return field;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <T extends Enum> EnumComboBox createEnumComboBox(Class<T> enumType) {
        return new EnumComboBox(enumType, this.allowNull);
    }

    // This method exists solely to bind the generic type parameters
    private <T> SimpleFieldConverter<T> buildSimpleFieldConverter(Encoding<T> encoding) {
        return new SimpleFieldConverter<>(encoding);
    }

    // This method exists solely to bind the generic type parameters
    private <T> NullableField<T> addNullButton(Field<T> field) {
        return new NullableField<>(field);
    }
}
