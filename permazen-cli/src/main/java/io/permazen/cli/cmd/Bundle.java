
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.SimpleCommandWrapper;
import io.permazen.util.ApplicationClassLoader;

import java.util.ServiceLoader;

import org.dellroad.jct.core.simple.TreeMapBundle;

@SuppressWarnings("serial")
public class Bundle extends TreeMapBundle {

    @SuppressWarnings("this-escape")
    public Bundle() {
        super("Permazen Commands");
        ServiceLoader.load(Command.class, ApplicationClassLoader.getInstance()).stream()
          .map(ServiceLoader.Provider::get)
          .iterator().forEachRemaining(this::addPermazenCommand);
    }

    public void addPermazenCommand(Command command) {
        this.put(command.getName(), new SimpleCommandWrapper(command));
    }
}
