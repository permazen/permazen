
/*
 * Copyright (C) 2014 Archie L. Cobbs. All rights reserved.
 *
 * $Id$
 */

package org.jsimpledb.cli;

import java.util.ArrayList;

@SuppressWarnings("serial")
public class Channels extends ArrayList<Channel<?>> {

    public Channels() {
    }

    public Channels(Channel<?> channel) {
        this.add(channel);
    }

    public Channels(Iterable<? extends Channel<?>> channels) {
        for (Channel<?> channel : channels)
            this.add(channel);
    }
}

