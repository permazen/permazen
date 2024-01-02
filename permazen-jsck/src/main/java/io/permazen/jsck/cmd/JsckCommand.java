
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.jsck.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.cli.cmd.AbstractCommand;
import io.permazen.jsck.Jsck;
import io.permazen.jsck.JsckConfig;
import io.permazen.jsck.JsckLogger;
import io.permazen.kv.KVStore;

import java.io.PrintStream;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class JsckCommand extends AbstractCommand {

    public JsckCommand() {
        super("jsck"
         + " -repair:repair"
         + " -verbose:verbose"
         + " -weak:weak"
         + " -limit:limit:int"
         + " -gc:gc"
/*
         + " -kv:kv:expr"
         + " -registry:registry:expr"
         + " -force-schemas:schema-map:expr"
*/
         + " -force-format-version:format-version:int");
    }

    @Override
    public String getHelpSummary() {
        return "Check key/value database for inconsistencies and optionally repair them";
    }

    @Override
    public String getHelpDetail() {
        return "Options:\n"
          + "   -repair\n"
          + "       In addition to detecting inconsistencies, attempt to repair them; without this flag,\n"
          + "       inconsistencies are only reported and no changes are made.\n"
          + "   -limit\n"
          + "       Stop after encountering `limit' issues.\n"
          + "   -gc\n"
          + "       Garbage collect any unused schema versions at the end of inspection.\n"
          + "       Note: this garbage collection will occur even without `-repair'.\n"
/*
          + "   -kv\n"
          + "       Specify a different KVStore to check (by default, the current transaction is checked).\n"
          + "   -registry\n"
          + "       Specify a custom encoding registry. If this flag is not given, in Permazen and Core API modes,\n"
          + "       the configured registry will be used; in key/value database CLI mode, a default instances is used.\n"
          + "       The parameter must be a Java expression returning an EncodingRegistry.\n"
          + "   -force-schemas\n"
          + "       Forcibly override schema versions. The parameter must be a Java expression returning a\n"
          + "       Map<Integer, SchemaModel>. WARNING: only use this if you know what you are doing.\n"
          + "       This flag is ignored without `-repair'.\n"
*/
          + "   -force-format-version\n"
          + "       Forcibly override format version. WARNING: only use this if you know what you are doing.\n"
          + "       This flag is ignored without `-repair'.\n"
          + "   -verbose\n"
          + "       Increase logging verbosity to show a high level of detail.\n"
          + "   -weak\n"
          + "       For certain key/value stores, use weaker consistency to reduce the chance of conflicts.\n"
          + "       This flag is incompatible with `-repair' and/or `-gc'.\n";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.allOf(SessionMode.class);
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {

        // Setup config (partially)
        final JsckConfig config = new JsckConfig();
        final boolean verbose = params.containsKey("verbose");
        config.setGarbageCollectSchemas(params.containsKey("gc"));
        config.setRepair(params.containsKey("repair"));
        final boolean weak = params.containsKey("weak");
        final Integer formatVersion = (Integer)params.get("format-version");
        if (formatVersion != null)
            config.setFormatVersionOverride(formatVersion);
        final Integer limit = (Integer)params.get("limit");
        if (limit != null)
            config.setMaxIssues(limit);

        // Sanity check
        if (weak && (config.isGarbageCollectSchemas() || config.isRepair()))
            throw new RuntimeException("`-weak' flag requires read-only transaction (incompatible with `-gc' and `-repair')");

        // Done
        return new JsckAction(config,
/*
          (Node)params.get("kv"), (Node)params.get("registry"), (Node)params.get("schema-map"),
*/
          verbose, weak);
    }

    private static class JsckAction implements Session.Action, Session.TransactionalAction, Session.HasTransactionOptions {

        private final JsckConfig config;
/*
        private final Node kvNode;
        private final Node registryNode;
        private final Node schemasNode;
*/
        private final boolean verbose;
        private final boolean weak;

        JsckAction(JsckConfig config, /*Node kvNode, Node registryNode, Node schemasNode,*/ boolean verbose, boolean weak) {
            this.config = config;
/*
            this.kvNode = kvNode;
            this.registryNode = registryNode;
            this.schemasNode = schemasNode;
*/
            this.verbose = verbose;
            this.weak = weak;
        }

        @Override
        public void run(Session session) throws Exception {

/*
            // Evaluate KVStore, if any
            final KVStore kv = this.kvNode != null ?
              JsckCommand.this.getExprParam(session, this.kvNode, "kv", KVStore.class) : session.getKVTransaction();

            // Evaluate registry, if any
            if (this.registryNode != null) {
                this.config.setEncodingRegistry(
                  JsckCommand.this.getExprParam(session, this.registryNode, "registry", EncodingRegistry.class));
            } else if (session.getDatabase() != null)
                config.setEncodingRegistry(session.getDatabase().getEncodingRegistry());

            // Evaluate forced schemas, if any
            if (this.schemasNode != null) {
                config.setForceSchemaVersions(JsckCommand.this.getExprParam(session, this.schemasNode, "force-schemas", obj -> {
                    if (!(obj instanceof Map))
                        throw new IllegalArgumentException("must be a Map<Integer, SchemaModel>");
                    final Map<?, ?> map = (Map<?, ?>)obj;
                    final HashMap<Integer, SchemaModel> versionMap = new HashMap<>(map.size());
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        if (!(entry.getKey() instanceof Integer))
                            throw new IllegalArgumentException("must be a Map<Integer, SchemaModel>; found key " + entry.getKey());
                        if ((entry.getValue() != null && !(entry.getValue() instanceof SchemaModel))) {
                            throw new IllegalArgumentException("must be a Map<Integer, SchemaModel>; found value "
                              + entry.getValue());
                        }
                        versionMap.put((Integer)entry.getKey(), (SchemaModel)entry.getValue());
                    }
                    return versionMap;
                }));
            }
*/
            final KVStore kv = session.getKVTransaction();
            if (session.getDatabase() != null)
                config.setEncodingRegistry(session.getDatabase().getEncodingRegistry());

            // Configure logger to log to console
            final PrintStream out = session.getOutput();
            this.config.setJsckLogger(new JsckLogger() {
                @Override
                public boolean isDetailEnabled() {
                    return verbose;
                }
                @Override
                public void info(String format, Object... args) {
                    out.println("jsck: " + String.format(format, args));
                }
                @Override
                public void detail(String format, Object... args) {
                    if (verbose)
                        out.println("jsck: " + String.format(format, args));
                }
            });

            // Do scan
            final Jsck jsck = new Jsck(this.config);
            final AtomicInteger count = new AtomicInteger();
            final long numHandled = jsck.check(kv, issue -> {
                out.println(String.format("[%05d] %s%s", count.incrementAndGet(),
                  issue, this.config.isRepair() ? " [FIXED]" : ""));
            });
            out.println("jsck: " + (this.config.isRepair() ? "repaired" : "found") + " " + numHandled + " issue(s)");
        }

        // Use EVENTUAL_COMMITTED consistency for Raft key/value stores to avoid retries
        @Override
        public Map<String, ?> getTransactionOptions() {
            return this.weak && !this.config.isRepair() ? Collections.singletonMap("consistency", "EVENTUAL") : null;
        }
    }
}
