
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.core.Schema;
import io.permazen.core.SchemaBundle;
import io.permazen.schema.SchemaId;
import io.permazen.schema.SchemaModel;

import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;

public class ShowSchemaCommand extends AbstractCommand {

    public ShowSchemaCommand() {
        super("show-schema --active:active --xml:xml --no-storage-ids:noStorageIds schemaId?");
    }

    @Override
    public String getHelpSummary() {
        return "Shows schema information.";
    }

    @Override
    public String getHelpDetail() {
        return "If \"--active\" is given, shows the schema configured for the current CLI session. Otheriwse, shows\n"
          + " the specified schema, or all schemas if no schema ID is specified, currently recorded in the database.\n"
          + "\n"
          + "With \"--xml\", the XML representation of each schema version is displayed including any explicit storage\n"
          + "ID's. You can use \"--no-storage-ids\" to omit them.";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {

        // Get flags
        final boolean active = params.containsKey("active");
        final boolean xml = params.containsKey("xml");
        final boolean noStorageIds = params.containsKey("noStorageIds");
        final SchemaId schemaId = Optional.ofNullable((String)params.get("schemaId")).map(SchemaId::new).orElse(null);

        // Sanity check flags
        if (active && schemaId != null)
            throw new IllegalArgumentException("invalid combination of \"--active\" flag with explicit schema ID");

        // Build action
        return active ?
          new ShowActiveSchemaAction(xml, !noStorageIds) :
          new ShowDatabaseSchemaAction(schemaId, xml, !noStorageIds);
    }

    private abstract static class AbstractShowSchemaAction implements Session.Action {

        protected final boolean xml;
        protected final boolean storageIds;

        protected AbstractShowSchemaAction(boolean xml, boolean storageIds) {
            this.xml = xml;
            this.storageIds = storageIds;
        }

        protected void printSchema(Session session, SchemaModel schemaModel) {
            if (this.xml) {
                session.getOutput().println(String.format("=== Schema version \"%s\" ===", schemaModel.getSchemaId()));
                session.getOutput().println(schemaModel.toString(this.storageIds, true));
            } else
                session.getOutput().println(schemaModel.getSchemaId());
        }
    }

    private static class ShowActiveSchemaAction extends AbstractShowSchemaAction {

        ShowActiveSchemaAction(boolean xml, boolean storageIds) {
            super(xml, storageIds);
        }

        @Override
        public void run(Session session) throws Exception {
            final SchemaModel schemaModel = SchemaUtil.getSchemaModel(session, null);
            if (schemaModel == null) {
                session.getOutput().println("No schema configured on this session");
                return;
            }
            this.printSchema(session, schemaModel);
        }
    }

    private static class ShowDatabaseSchemaAction extends AbstractShowSchemaAction {

        private final SchemaId schemaId;

        ShowDatabaseSchemaAction(SchemaId schemaId, boolean xml, boolean storageIds) {
            super(xml, storageIds);
            this.schemaId = schemaId;
        }

        @Override
        public void run(Session session) throws Exception {
            final SchemaBundle schemaBundle = SchemaUtil.readSchemaBundle(session);
            if (this.schemaId == null) {
                schemaBundle.getSchemasBySchemaId().values().stream()
                  .map(Schema::getSchemaModel)
                  .forEach(schemaModel -> this.printSchema(session, schemaModel));
            } else {
                final Schema schema = schemaBundle.getSchema(this.schemaId);
                if (schema == null) {
                    session.getOutput().println(String.format(
                      "Schema \"%s\" not found; known versions are: %s",
                      schemaId, schemaBundle.getSchemasBySchemaId().keySet()));
                } else
                    this.printSchema(session, schema.getSchemaModel());
            }
        }
    }
}
