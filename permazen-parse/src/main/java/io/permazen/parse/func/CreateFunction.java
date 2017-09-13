
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.parse.func;

import io.permazen.JTransaction;
import io.permazen.core.ObjId;
import io.permazen.core.Transaction;
import io.permazen.parse.ObjTypeParser;
import io.permazen.parse.ParseException;
import io.permazen.parse.ParseSession;
import io.permazen.parse.expr.ConstValue;
import io.permazen.parse.expr.Node;
import io.permazen.parse.expr.Value;
import io.permazen.util.ParseContext;

public class CreateFunction extends AbstractFunction {

    public CreateFunction() {
        super("create");
    }

    @Override
    public String getHelpSummary() {
        return "Creates a new database object instance";
    }

    @Override
    public String getUsage() {
        return "create(type [, version ])";
    }

    @Override
    public String getHelpDetail() {
        return "Creates and returns a new instance of the specified type. The optional `version' parameter forces"
          + " a specific schema version.";
    }

    @Override
    public ParamInfo parseParams(ParseSession session, ParseContext ctx, boolean complete) {

        // Get object type
        if (ctx.tryLiteral(")"))
            throw new ParseException(ctx, "type parameter required");
        final int storageId = new ObjTypeParser().parse(session, ctx, complete).getStorageId();

        // Check for optional version
        this.spaceParser.parse(ctx, complete);
        if (ctx.tryLiteral(")"))
            return new ParamInfo(storageId);
        if (!ctx.tryLiteral(","))
            throw new ParseException(ctx, "expected `,' between function parameters").addCompletion(", ");

        // Get version
        this.spaceParser.parse(ctx, complete);
        final Node version = this.parseExpressionParams(session, ctx, complete, 1, 1, 1)[0];

        // Done
        return new ParamInfo(storageId, version);
    }

    @Override
    public Value apply(ParseSession session, Object params) {
        final ParamInfo info = (ParamInfo)params;
        final int storageId = info.getStorageId();
        final Node version = info.getVersion();

        // Create object
        final Transaction tx = session.getTransaction();
        final ObjId id = version != null ?
          tx.create(storageId, version.evaluate(session).checkIntegral(session, "create()")) : tx.create(storageId);
        return new ConstValue(session.getMode().hasPermazen() ? JTransaction.getCurrent().get(id) : id);
    }

// ParamInfo

    private static class ParamInfo {

        private final int storageId;
        private final Node version;

        ParamInfo(int storageId) {
            this(storageId, null);
        }

        ParamInfo(int storageId, Node version) {
            this.storageId = storageId;
            this.version = version;
        }

        public int getStorageId() {
            return this.storageId;
        }

        public Node getVersion() {
            return this.version;
        }
    }
}
