
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.jsck;

import org.jsimpledb.schema.CollectionSchemaField;

abstract class CollectionElementIndex extends ComplexFieldIndex {

    protected CollectionElementIndex(JsckInfo info, int schemaVersion, CollectionSchemaField field, String parentFieldName) {
        super(info, schemaVersion, field, field.getElementField(), parentFieldName, "element");
    }
}

