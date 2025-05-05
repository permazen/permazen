// Imports
import io.permazen.*;
import io.permazen.core.*;
import io.permazen.kv.*;
import io.permazen.util.*;

// Set a variable for the JShell session
var jshell = io.permazen.cli.jshell.PermazenJShellShellSession.getCurrent();

// Set a variable for the Permazen session
var session = jshell.getPermazenSession();

// Add convenience methods for managing snippet transactions
void begin() {
    jshell.begin();
}
void commit() {
    jshell.commit();
}
void rollback() {
    jshell.rollback();
}
void branch() {
    jshell.branch();
}
void branch(java.util.Map<String, ?> openOptions, java.util.Map<String, ?> syncOptions) {
    jshell.branch(openOptions, syncOptions);
}

// Emit a greeting
System.out.println();
System.out.println(String.format(
  "|  Welcome to the Permazen %s version of JShell.", Permazen.VERSION));
System.out.println("""

You can access the current io.permazen.cli.Session instance as "session", and the current
io.permazen.cli.jshell.PermazenJShellShellSession instance as "jshell".

In this version of JShell, snippets execute in the context of an open Permazen transaction.
By default, a new transaction is automatically created before, and committed after, each snippet
execution, or rolled back if an exception is thrown. Alternatively, you may create "extended"
transactions that remain open until you close them (these can also be branched transactions).

Use these "commands" to manage extended transactions:

  begin()     Start a normal extended transaction
  branch()    Start a branched extended transaction
  commit()    Commit the current extended transaction
  rollback()  Abort the current extended transaction (if any)

As long as an extended transaction remains open, snippets will particiate in it.
""");
