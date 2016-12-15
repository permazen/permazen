
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.cli.cmd;

import org.jsimpledb.JSimpleDB;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.core.Schema;
import org.jsimpledb.core.SchemaMismatchException;
import org.jsimpledb.core.Transaction;
import org.jsimpledb.schema.SchemaModel;

abstract class AbstractSchemaCommand extends AbstractCommand {

    AbstractSchemaCommand(String spec) {
        super(spec);
    }

    // Get the schema having the specified version
    protected static SchemaModel getSchemaModel(CliSession session, final int version) {

        // Version zero means "the configured schema"
        if (version == 0) {
            SchemaModel schemaModel = session.getSchemaModel();
            if (schemaModel == null) {
                final JSimpleDB jdb = session.getJSimpleDB();
                if (jdb != null)
                    schemaModel = jdb.getSchemaModel();
            }
            if (schemaModel == null) {
                session.getWriter().println("No schema configured on this session");
                return null;
            }
            return schemaModel;
        }

        // Read schema from the database
        return AbstractSchemaCommand.runWithoutSchema(session, (session1, tx) -> {
            final Schema schema = tx.getSchemas().getVersions().get(version);
            if (schema == null) {
                session1.getWriter().println("Schema version " + version
                  + " not found (known versions: " + tx.getSchemas().getVersions().keySet() + ")");
                return null;
            }
            return schema.getSchemaModel();
        });
    }

    // Perform action in a transaction that doesn't have any preconceived notion of what schema(s) should be in there
    protected static <R> R runWithoutSchema(CliSession session, SchemaAgnosticAction<R> action) {

        final Transaction tx;
        try {
            tx = session.getDatabase().createTransaction(null, 0, false);
        } catch (SchemaMismatchException e) {                                   // must be a uninitialized database
            session.getWriter().println("Database is uninitialized");
            return null;
        }
        boolean success = false;
        try {
            final R result = action.runWithoutSchema(session, tx);
            tx.commit();
            success = true;
            return result;
        } finally {
            if (!success)
                tx.rollback();
        }
    }

    protected interface SchemaAgnosticAction<R> {
        R runWithoutSchema(CliSession session, Transaction tx);
    }
}

