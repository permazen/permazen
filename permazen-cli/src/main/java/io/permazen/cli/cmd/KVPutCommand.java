
/*
 * Copyright (C) 2015 Archie L. Cobbs. All rights reserved.
 */

package io.permazen.cli.cmd;

import io.permazen.cli.Session;
import io.permazen.cli.SessionMode;
import io.permazen.util.ByteData;

import java.util.EnumSet;
import java.util.Map;

public class KVPutCommand extends AbstractKVCommand {

    public KVPutCommand() {
        super("kvput key:bytes value:bytes");
    }

    @Override
    public String getHelpSummary() {
        return "Write a raw database key/value pair";
    }

    @Override
    public String getHelpDetail() {
        return "Writes a raw database key/value pair. The \"key\" and \"value\" parameters may be given as a hexadecimal value"
          + " or C-style doubly-quoted string."
          + "\n\nWARNING: this command can corrupt a Permazen database.";
    }

    @Override
    public EnumSet<SessionMode> getSessionModes() {
        return EnumSet.of(SessionMode.KEY_VALUE);
    }

    @Override
    public Session.Action getAction(Session session, Map<String, Object> params) {
        return new PutAction((ByteData)params.get("key"), (ByteData)params.get("value"));
    }

    private static class PutAction implements KVAction {

        private final ByteData key;
        private final ByteData value;

        PutAction(ByteData key, ByteData value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public void run(Session session) throws Exception {
            session.getKVTransaction().put(this.key, this.value);
        }
    }
}
