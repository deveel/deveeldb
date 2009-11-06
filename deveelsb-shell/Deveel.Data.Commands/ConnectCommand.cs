using System;

using Deveel.Commands;
using Deveel.Configuration;
using Deveel.Data.Client;
using Deveel.Data.Shell;
using Deveel.Shell;

namespace Deveel.Data.Commands {
	[Command("connect", ShortDescription = "connects to a server.")]
	[CommandSynopsis("connect <conn-string> [ <alias> ]")]
	[CommandGroup("sessions")]
	public sealed class ConnectCommand : Command {
		public override string LongDescription {
			get {
				return "connects to the server with the optional session name.\n" + 
					"If no session name is given, a session name is chosen.\n" + 
					"If a session name is given, this is stored as an alias\n" + 
					"for the string as well, so later you might connect with\n" + 
					"that alias conveniently instead:\n" + 
					"\tconnect \"Host=192.168.0.1;User=SA;Database=foo\" myAlias\n" + 
					"allows to later connect simply with\n" + 
					"\tconnect myAlias\n" + 
					"Of course, all strings and aliases are stored in your \n" + 
					"~/.deveeldb configuration. All connects and aliases \n" + 
					"are provided in the TAB-completion of this command.";
			}
		}

		public override bool HandleCommandLine(CommandLine commandLine) {
			if (!commandLine.HasOption("host"))
				return false;

			string host = null;
			string username = null;
			string password = null;
			int port = ConnectionString.DefaultPort;
			string database = null;

			string[] args = commandLine.Arguments;

			if (args.Length > 0) {
				host = args[0];
				username = (args.Length > 1) ? args[1] : null;
				password = (args.Length > 2) ? args[2] : null;

			}
			if (commandLine.HasOption("u")) {
				username = commandLine.GetOptionValue("u");
			}
			if (commandLine.HasOption("p")) {
				password = commandLine.GetOptionValue("p");
				if (password == null)
					password = Readline.ReadPassword("Password: ");
			}
			if (commandLine.HasOption("h")) {
				host = commandLine.GetOptionValue("h", ConnectionString.LocalHost);
				int index = host.IndexOf(':');
				if (index != -1) {
					string sPort = host.Substring(index + 1);
					host = host.Substring(0, index);
					port = Int32.Parse(sPort);
				}
			}
			if (commandLine.HasOption("d")) {
				database = commandLine.GetOptionValue("d");
			}

			if (host != null) {
				try {
					Connect(host, port, database, username, password);
					return true;
				} catch (Exception e) {
					OutputDevice.Message.WriteLine(e.Message);
					return false;
				}
			}

			return false;
		}

		public override void RegisterOptions(CommandLineOptions options) {
			CommandLineOption option = new CommandLineOption("h", "host", true, "Host to connect to");
			option.ArgumentName = "address";
			options.AddOption(option);
		}

		private void Connect(string host, int port, string database, string username, string password) {
			ConnectionString connectionString = new ConnectionString(host, port, username, password);
			if (database != null)
				connectionString.Database = database;

			string connString = connectionString.ToString();

			DeveelDBShell shell = (DeveelDBShell) Application;

			shell.CreateSession(connString, null);
			shell.SetDefaultPrompt();
		}

		public override System.Collections.IEnumerator Complete(CommandDispatcher dispatcher, string partialCommand, string lastWord) {
			DeveelDBShell shell = (DeveelDBShell)Application;
			return shell.Connections.Complete(lastWord);
		}

		public override CommandResultCode Execute(object context, string[] args) {
			int argc = args.Length;
			if (argc < 1 || argc > 2)
				return CommandResultCode.SyntaxError;

			DeveelDBShell shell = (DeveelDBShell)Application;

			String connectionString = args[0];
			if (connectionString[0] == '\"' &&
				connectionString[connectionString.Length - 1] == '\"')
				connectionString = connectionString.Substring(1, connectionString.Length - 2);

			String alias = (argc == 2) ? args[1] : null;
			if (alias == null) {
				// we only got one parameter. So the that single parameter
				// might have been an alias. let's see..
				if (shell.Connections.HasConnection(connectionString)) {
					String possibleAlias = connectionString;
					connectionString = shell.Connections.GetConnectionString(connectionString);
					if (!possibleAlias.Equals(connectionString)) {
						alias = possibleAlias;
					}
				}
			}
			try {
				SqlSession session = shell.CreateSession(connectionString, alias);
				if (session == null) {
					Application.MessageDevice.WriteLine("unable to connect to the database");
					return CommandResultCode.ExecutionFailed;
				}
				if (alias != null)
					shell.Connections.AddConnectionString(alias, connectionString);
				return CommandResultCode.Success;
			} catch (Exception e) {
				OutputDevice.Message.WriteLine(e.Message);
				return CommandResultCode.ExecutionFailed;
			}
		}
	}
}