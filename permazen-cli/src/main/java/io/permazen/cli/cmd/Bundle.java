
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.SimpleCommandWrapper;
import io.permazen.util.ImplementationsReader;

import java.util.ArrayList;

import org.dellroad.jct.core.simple.TreeMapBundle;

@SuppressWarnings("serial")
public class Bundle extends TreeMapBundle {

    /**
     * Classpath XML file resource describing available {@link Command}s: {@value #CLI_COMMANDS_DESCRIPTOR_RESOURCE}.
     *
     * <p>
     * Example:
     * <blockquote><pre>
     *  &lt;cli-command-implementations&gt;
     *      &lt;cli-command-implementation class="com.example.MyCliCommand"/&gt;
     *  &lt;/cli-command-implementations&gt;
     * </pre></blockquote>
     *
     * <p>
     * Instances must have a public default constructor.
     */
    public static final String CLI_COMMANDS_DESCRIPTOR_RESOURCE = "META-INF/permazen/cli-command-implementations.xml";

    public Bundle() {
        super("Permazen Commands");
        final ImplementationsReader reader = new ImplementationsReader("cli-command");
        final ArrayList<Object[]> paramLists = new ArrayList<>(1);
        paramLists.add(new Object[0]);
        reader.setConstructorParameterLists(paramLists);
        reader.findImplementations(Command.class, CLI_COMMANDS_DESCRIPTOR_RESOURCE).forEach(this::addPermazenCommand);
    }

    public void addPermazenCommand(Command command) {
        this.put(command.getName(), new SimpleCommandWrapper(command));
    }
}
