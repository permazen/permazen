
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.Permazen;
import io.permazen.cli.Session;
import io.permazen.core.Schema;
import io.permazen.core.SchemaBundle;
import io.permazen.kv.KVTransaction;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;

final class SchemaUtil {

    private SchemaUtil() {
    }

    public static SchemaModel getSchemaModel(Session session, final SchemaId schemaId) {

        // Null means "the configured schema"
        if (schemaId == null) {
            SchemaModel schemaModel = session.getSchemaModel();
            if (schemaModel == null) {
                final Permazen pdb = session.getPermazen();
                if (pdb != null)
                    schemaModel = pdb.getSchemaModel();
            }
            return schemaModel;
        }

        // Read schema from the database
        final SchemaBundle schemaBundle = SchemaUtil.readSchemaBundle(session);
        final Schema schema = schemaBundle.getSchema(schemaId);
        if (schema == null) {
            session.getOutput().println(String.format(
              "Schema \"%s\" not found in database (known versions: %s)",
              schemaId, schemaBundle.getSchemasBySchemaId().keySet()));
            return null;
        }
        return schema.getSchemaModel();
    }

    // Read schema info from database in a key/value transaction to avoid core API which could modify the schema table
    public static SchemaBundle readSchemaBundle(Session session) {
        final KVTransaction kvt = session.getKVDatabase().createTransaction();
        try {
            final SchemaBundle schemaBundle = new SchemaBundle(SchemaBundle.Encoded.readFrom(kvt));
            kvt.commit();
            return schemaBundle;
        } finally {
            kvt.rollback();
        }
    }
}
