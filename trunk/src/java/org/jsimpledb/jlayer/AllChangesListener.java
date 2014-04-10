
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.jlayer;

import org.jsimpledb.ListFieldChangeListener;
import org.jsimpledb.MapFieldChangeListener;
import org.jsimpledb.SetFieldChangeListener;
import org.jsimpledb.SimpleFieldChangeListener;

interface AllChangesListener extends SimpleFieldChangeListener, SetFieldChangeListener,
  ListFieldChangeListener, MapFieldChangeListener {
}

