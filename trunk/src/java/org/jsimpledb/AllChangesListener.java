
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb;

import org.jsimpledb.core.ListFieldChangeListener;
import org.jsimpledb.core.MapFieldChangeListener;
import org.jsimpledb.core.SetFieldChangeListener;
import org.jsimpledb.core.SimpleFieldChangeListener;

interface AllChangesListener extends SimpleFieldChangeListener, SetFieldChangeListener,
  ListFieldChangeListener, MapFieldChangeListener {
}

