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
				                    	"connect <connection-string> [ <alias> ]",
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
			throw new NotImplementedException();
		}
	}
}