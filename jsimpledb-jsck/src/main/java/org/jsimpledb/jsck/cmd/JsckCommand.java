
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package org.jsimpledb.jsck.cmd;

import java.io.PrintWriter;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.jsimpledb.Session;
import org.jsimpledb.SessionMode;
import org.jsimpledb.cli.CliSession;
import org.jsimpledb.cli.cmd.AbstractCommand;
import org.jsimpledb.core.FieldTypeRegistry;
import org.jsimpledb.jsck.Jsck;
import org.jsimpledb.jsck.JsckConfig;
import org.jsimpledb.jsck.JsckLogger;
import org.jsimpledb.kv.KVTransaction;
import org.jsimpledb.schema.SchemaModel;
import org.jsimpledb.util.ParseContext;

public class JsckCommand extends AbstractCommand {

    public JsckCommand() {
        super("jsck"
         + " -repair:repair"
         + " -verbose:verbose"
         + " -limit:limit:int"
         + " -gc:garbageCollectSchemas"
         + " -force-schemas:schema-map:expr"
         + " -force-format-version:format-version:int"
         + " -registry:registry:expr");
    }

    @Override
    public String getHelpSummary() {
        return "Check a key/value database for inconsistencies and optionally repair them";
    }

    @Override
    public String getHelpDetail() {
        return "Options:\n"
          + "   -repair\n"
          + "       In addition to detecting issues, attempt to repair them.\n"
          + "   -limit\n"
          + "       Stop after encountering `limit' issues.\n"
          + "   -gc\n"
          + "       Garbage collect unused schema versions at the end of inspection.\n"
          + "   -registry\n"
          + "       Specify a custom field type registry. If this flag is not given, in JSimpleDB and Core API modes,\n"
          + "       the configured registry will be used; in key/value database CLI mode, a default instances is used.\n"
          + "       The parameter must be a Java expression returning a FieldTypeRegistry.\n"
          + "   -force-schemas\n"
          + "       Forcibly override schema versions. WARNING: only use this if you know what you are doing.\n"
          + "       The parameter must be a Java expression returning a Map<Integer, SchemaModel>.\n"
          + "       This flag is ignored without `-repair'.\n"
          + "   -force-format-version\n"
          + "       Forcibly override format version. WARNING: only use this if you know what you are doing.\n"
          + "       This flag is ignored without `-repair'.\n"
          + "   -verbose\n"
          + "       Increase logging verbosity to show a high level of detail\n";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public CliSession.Action getAction(CliSession session, ParseContext ctx, boolean complete, Map<String, Object> params) {

        // Setup config
        final JsckConfig config = new JsckConfig();
        final boolean verbose = params.containsKey("verbose");
        config.setGarbageCollectSchemas(params.containsKey("gc"));
        final FieldTypeRegistry registry = this.getParam(params, "registry", FieldTypeRegistry.class);
        if (registry != null)
            config.setFieldTypeRegistry(registry);
        config.setRepair(params.containsKey("repair"));
        final Integer formatVersion = (Integer)params.get("force-format-version");
        if (formatVersion != null)
            config.setForceFormatVersion(formatVersion);
        final Integer limit = (Integer)params.get("limit");
        if (limit != null)
            config.setMaxIssues(limit);
        final Map<?, ?> forceSchemaVersions = this.getParam(params, "force-schemas", Map.class, "Map<Integer, SchemaModel>");
        if (forceSchemaVersions != null) {
            final HashMap<Integer, SchemaModel> versionMap = new HashMap<>(forceSchemaVersions.size());
            for (Map.Entry<?, ?> entry : forceSchemaVersions.entrySet()) {
                if (!(entry.getKey() instanceof Integer)
                  || (entry.getValue() != null && !(entry.getValue() instanceof SchemaModel)))
                    throw new IllegalArgumentException("parameter to `-force-schemas' must be a Map<Integer, SchemaModel>");
                versionMap.put((Integer)entry.getKey(), (SchemaModel)entry.getValue());
            }
            config.setForceSchemaVersions(versionMap);
        }

        // Done
        return new JsckAction(config, verbose);
    }

    private <T> T getParam(Map<String, Object> params, String name, Class<T> type) {
        return this.getParam(params, name, type, type.getSimpleName());
    }

    private <T> T getParam(Map<String, Object> params, String name, Class<T> type, String typeDescription) {
        final Object param = params.get(name);
        if (param == null)
            return null;
        if (!type.isInstance(param))
            throw new IllegalArgumentException("parameter to `-" + name + "' must be a " + typeDescription);
        return type.cast(param);
    }

    private static class JsckAction implements CliSession.Action, Session.TransactionalAction {

        private final JsckConfig config;
        private final boolean verbose;

        JsckAction(JsckConfig config, boolean verbose) {
            this.config = config;
            this.verbose = verbose;
        }

        @Override
        public void run(CliSession session) throws Exception {

            // Configure logger to log to consol
            final PrintWriter writer = session.getWriter();
            this.config.setJsckLogger(new JsckLogger() {
                @Override
                public boolean isDetailEnabled() {
                    return verbose;
                }
                @Override
                public void info(String message) {
                    writer.println("jsck: " + message);
                }
                @Override
                public void detail(String message) {
                    if (verbose)
                        writer.println("jsck: " + message);
                }
            });

            // Do scan
            final KVTransaction kvt = session.getKVTransaction();
            final Jsck jsck = new Jsck(this.config);
            final AtomicInteger count = new AtomicInteger();
            final long numHandled = jsck.check(kvt, issue -> {
                writer.println(String.format("[%05d] %s%s", count.incrementAndGet(),
                  issue, this.config.isRepair() ? " [FIXED]" : ""));
            });
            writer.println("jsck: " + (this.config.isRepair() ? "repaired" : "found") + " " + numHandled + " issue(s)");
        }
    }
}

