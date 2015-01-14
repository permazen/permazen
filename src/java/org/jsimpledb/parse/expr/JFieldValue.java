
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.parse.expr;

import org.jsimpledb.JField;
import org.jsimpledb.JObject;
import org.jsimpledb.parse.ParseSession;

/**
 * Value that reflects a {@link JField} in some {@link JObject}.
 */
public class JFieldValue extends AbstractValue {

    protected final JObject jobj;
    protected final JField jfield;

    /**
     * Constructor.
     *
     * @param jobj database object
     * @param jfield database field
     * @throws IllegalArgumentException if {@code jobj} is null
     * @throws IllegalArgumentException if {@code jfield} is null
     */
    public JFieldValue(JObject jobj, JField jfield) {
        if (jobj == null)
            throw new IllegalArgumentException("null jobj");
        if (jfield == null)
            throw new IllegalArgumentException("null jfield");
        this.jobj = jobj;
        this.jfield = jfield;
    }

    @Override
    public Object get(ParseSession session) {
        try {
            return this.jfield.getValue(this.jobj);
        } catch (Exception e) {
            throw new EvalException("error reading field `" + this.jfield.getName() + "' from object " + this.jobj.getObjId()
              + ": " + (e.getMessage() != null ? e.getMessage() : e));
        }
    }
}

