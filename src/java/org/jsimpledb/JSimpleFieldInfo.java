
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

class JSimpleFieldInfo extends JFieldInfo {

    private JComplexFieldInfo parent;

    JSimpleFieldInfo(JSimpleField jfield, JComplexFieldInfo parent) {
        super(jfield);
        this.parent = parent;
    }

    public JComplexFieldInfo getParent() {
        return this.parent;
    }

// Object

    @Override
    public String toString() {
        final String prefix = this.getClass().getSimpleName().replaceAll("^J(.+)FieldInfo", "$1 ").toLowerCase();
        return prefix + (this.parent != null ? "sub-" : "") + super.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this)
            return true;
        if (!super.equals(obj))
            return false;
        final JSimpleFieldInfo that = (JSimpleFieldInfo)obj;
        return this.parent != null ? that.parent != null && this.parent.storageId == that.parent.storageId : that.parent == null;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ (this.parent != null ? this.parent.storageId : 0);
    }
}

