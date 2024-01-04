// Set variable with which we can access the Permazen session
var session = io.permazen.cli.jshell.PermazenJShellShellSession.getCurrent().getPermazenSession();

// Emit a greeting
System.out.println();
System.out.println("Welcome to Permazen JShell. Your Permazen CLI session is available via \"session\".");
System.out.println();
