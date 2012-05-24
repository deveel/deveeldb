//
//  Copyright 2011 Deveel
//
//  This file is part of DeveelDBShell.
//
//  DeveelDBShell is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  DeveelDBShell is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with DeveelDBShell. If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.Collections.Generic;

using Deveel.Configuration;
using Deveel.Console;
using Deveel.Console.Commands;
using Deveel.Data.Client;

namespace Deveel.Data.Commands {
	public sealed class ConnectCommand : Command {
		public override string Name {
			get { return "connect"; }
		}

		public override string ShortDescription {
			get { return "connects to a data source given"; }
		}

		public override string LongDescription {
			get {
				return "Connects to the data source with the optional session name. " +
					   "If no session name is given, a session name is chosen. " +
					   "If a session name is given, this is stored as an alias " +
					   "for the string as well, so later you might connect with " +
					   "that alias conveniently instead:\n" +
					   "\tconnect \"Host=192.168.0.1;User=SA;Database=foo\" myAlias\n" +
					   "allows to later connect simply with\n" +
					   "\tconnect myAlias\n" +
					   "Of course, all strings and aliases are stored in your " +
					   "~/.deveeldb configuration. All connects and aliases " +
					   "are provided in the TAB-completion of this command.";
			}
		}

		public override string[] Synopsis {
			get {
				return new string[] {
				                    	"connect <user> to <host> [ identified by <password> ] [ using <arg0> ... <argn> ] [ as <alias> ]",
				                    	"connect <alias>"
				                    };
			}
		}

		private void Connect(string host, int port, string database, string username, string password) {
			DeveelDbConnectionStringBuilder connectionString = new DeveelDbConnectionStringBuilder();
			connectionString.Host = host;
			connectionString.Port = port;
			connectionString.UserName = username;
			connectionString.Password = password;
			if (database != null)
				connectionString.Database = database;

			string connString = connectionString.ToString();

			DeveelDBShell shell = (DeveelDBShell)Application;

			//TODO: create the session
		}

		public override bool HandleCommandLine(CommandLine commandLine) {
			if (!commandLine.HasOption("host"))
				return false;

			string host = null;
			string username = null;
			string password = null;
			int port = -1;
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
				host = commandLine.GetOptionValue("h", "{Local}");
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
					Application.Error.WriteLine(e.Message);
					return false;
				}
			}

			return false;
		}

		public override void RegisterOptions(Options options) {
			options.AddOption("h", "host", true, "The host of the data source to connect to");
			base.RegisterOptions(options);
		}

		public override string GroupName {
			get { return "sessions"; }
		}

		public override CommandResultCode Execute(IExecutionContext context, CommandArguments args) {
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;

			string userName = args.Current;

			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;
			if (!String.Equals(args.Current, "to", StringComparison.CurrentCultureIgnoreCase))
				return CommandResultCode.SyntaxError;
			
			if (!args.MoveNext())
				return CommandResultCode.SyntaxError;

			string host = args.Current;
			int port = -1;

			if (String.IsNullOrEmpty(host))
				return CommandResultCode.SyntaxError;

			int index = host.IndexOf(':');
			if (index != -1) {
				string sPort = host.Substring(0, index);
				if (!Int32.TryParse(sPort, out port))
					return CommandResultCode.SyntaxError;
			}

			string password = null;
			bool usingPassword = false;

			Dictionary<string, string> connStringArgs = null;

			if (args.MoveNext()) {
				if (String.Equals(args.Current, "identified", StringComparison.InvariantCultureIgnoreCase)) {
					if (!args.MoveNext())
						return CommandResultCode.SyntaxError;
					if (!args.MoveNext())
						return CommandResultCode.SyntaxError;
					if (!String.Equals(args.Current, "by", StringComparison.InvariantCultureIgnoreCase))
						return CommandResultCode.SyntaxError;

					password = args.Current;
				} else if (String.Equals(args.Current, "using", StringComparison.InvariantCultureIgnoreCase)) {
					connStringArgs = new Dictionary<string, string>();

					while (args.MoveNext()) {
						string arg = args.Current;
						if (String.IsNullOrEmpty(arg))
							return CommandResultCode.SyntaxError;

						if (String.Equals(arg, "password", StringComparison.InvariantCultureIgnoreCase)) {
							usingPassword = true;
						} else if (String.Equals(arg, "as", StringComparison.InvariantCultureIgnoreCase)) {
							if (!args.MoveNext())
								return CommandResultCode.SyntaxError;

							break;
						} else {
							index = arg.IndexOf(':');
							if (index == -1)
								return CommandResultCode.SyntaxError;

							string key = arg.Substring(0, index);
							string value = arg.Substring(index + 1);

							connStringArgs.Add(key, value);
						}
					}
				} else if (String.Equals(args.Current, "as", StringComparison.InvariantCultureIgnoreCase)) {
				} else {
					return CommandResultCode.SyntaxError;
				}
			}

			if (String.IsNullOrEmpty(password) && usingPassword) {
				Out.WriteLine();
				password = Readline.ReadPassword("password: ");
			}

			DeveelDbConnectionStringBuilder connectionStringBuilder = new DeveelDbConnectionStringBuilder();
			connectionStringBuilder.Add("UserName", userName);
			connectionStringBuilder.Add("Host", host);
			if (port != -1)
				connectionStringBuilder.Add("Port", port);
			if (!String.IsNullOrEmpty(password))
				connectionStringBuilder.Add("Password", password);
		}
	}
}