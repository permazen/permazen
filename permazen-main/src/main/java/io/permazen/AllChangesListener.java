
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen;

import io.permazen.core.ListFieldChangeListener;
import io.permazen.core.MapFieldChangeListener;
import io.permazen.core.SetFieldChangeListener;
import io.permazen.core.SimpleFieldChangeListener;

interface AllChangesListener extends SimpleFieldChangeListener, SetFieldChangeListener,
  ListFieldChangeListener, MapFieldChangeListener {
}
