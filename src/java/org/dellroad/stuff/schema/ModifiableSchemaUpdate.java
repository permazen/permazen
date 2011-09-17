
/*
 * Copyright (C) 2011 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.dellroad.stuff.schema;

import java.util.Set;

/**
 * Extension of the {@link SchemaUpdate} interface with modifiable properties name and predecessor properties.
 */
public interface ModifiableSchemaUpdate extends SchemaUpdate {

    void setName(String name);

    void setRequiredPredecessors(Set<SchemaUpdate> requiredPredecessors);

    void setSingleAction(boolean singleAction);
}

