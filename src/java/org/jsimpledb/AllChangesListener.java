
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb;

import org.jsimpledb.core.ListFieldChangeListener;
import org.jsimpledb.core.MapFieldChangeListener;
import org.jsimpledb.core.SetFieldChangeListener;
import org.jsimpledb.core.SimpleFieldChangeListener;

interface AllChangesListener extends SimpleFieldChangeListener, SetFieldChangeListener,
  ListFieldChangeListener, MapFieldChangeListener {
}

