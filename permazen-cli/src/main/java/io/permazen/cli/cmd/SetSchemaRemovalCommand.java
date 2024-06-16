
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.parse.EnumNameParser;
import io.permazen.cli.parse.Parser;
import io.permazen.core.TransactionConfig;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SetSchemaRemovalCommand extends AbstractCommand {

    public SetSchemaRemovalCommand() {
        super("set-schema-removal policy:policy");
    }

    @Override
    public String getHelpSummary() {
        return "Sets the unused schema removal policy; One of "
          + Stream.of(TransactionConfig.SchemaRemoval.values())
              .map(m -> String.format("\"%s\"", m))
              .collect(Collectors.joining(", "));
    }

    @Override
    protected Parser<?> getParser(String typeName) {
        return "policy".equals(typeName) ?
          new EnumNameParser<>(TransactionConfig.SchemaRemoval.class, false) : super.getParser(typeName);
    }

    @Override
    public Session.Action getAction(Session session0, Map<String, Object> params) {
        final TransactionConfig.SchemaRemoval policy = (TransactionConfig.SchemaRemoval)params.get("policy");
        return session -> {
            session.setSchemaRemoval(policy);
            session.getOutput().println("Set schema removal to " + policy);
        };
    }
}
