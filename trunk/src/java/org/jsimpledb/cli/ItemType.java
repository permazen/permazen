
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import com.google.common.reflect.TypeToken;

import java.io.IOException;

public interface ItemType<T> {

    TypeToken<T> getTypeToken();

    void print(Session session, T item) throws IOException;
}

