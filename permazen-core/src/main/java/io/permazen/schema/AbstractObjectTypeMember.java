
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.schema;

abstract class AbstractObjectTypeMember extends SchemaItem {

    private SchemaObjectType objectType;

    /**
     * Get the {@link SchemaObjectType} of which this field is a member.
     *
     * @return containing object type
     */
    public SchemaObjectType getObjectType() {
        return this.objectType;
    }

    /**
     * Set the {@link SchemaObjectType} of which this field is a member.
     *
     * <p>
     * Note: this field is considered derived information, and will be set automatically
     * when a referrring {@link SchemaObjectType} is locked down.
     *
     * @param objectType containing object type
     * @throws UnsupportedOperationException if this instance is locked down
     */
    public final void setObjectType(SchemaObjectType objectType) {
        this.verifyNotLockedDown(false);
        this.objectType = objectType;
    }

// Cloneable

    @Override
    public AbstractObjectTypeMember clone() {
        return (AbstractObjectTypeMember)super.clone();
    }
}
