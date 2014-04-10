
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.core;

import java.util.NavigableSet;

interface FieldChangeNotifier {

    int getStorageId();

    ObjId getId();

    void notify(Transaction tx, Object listener, int[] path, NavigableSet<ObjId> referrers);
}

