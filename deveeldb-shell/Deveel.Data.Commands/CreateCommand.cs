using System;
using System.Collections;

using Deveel.Commands;
using Deveel.Configuration;
using Deveel.Data.Client;
using Deveel.Data.Shell;

namespace Deveel.Data.Commands {
	[Command("create", ShortDescription = "creates a database or a database object.")]
	[CommandSynopsis("create database <name> user <user> [ identified by <password> ] [ <var>=<value> ... ]")]
	[CommandSynopsis("create table | view | trigger ... ")]
	[Option("create", false, "creates a new database")]
	[OptionGroup("commands")]
	class CreateCommand : SqlCommand {
		// we disable the context for this command, since we want to 
		// enable the function of creating a database...
		public override bool RequiresContext {
			get { return false; }
		}

		public override CommandResultCode Execute(object context, string[] args) {
			if (args.Length < 2)
				return CommandResultCode.SyntaxError;

			if (String.Compare(args[0], "database", true) == 0) {
				if (args.Length < 4)
					return CommandResultCode.SyntaxError;

				string lastArg = args[args.Length - 1];
				if (lastArg[lastArg.Length - 1] == '/')
					args[args.Length - 1] = lastArg.Substring(0, lastArg.Length - 1).Trim();

				string dbName = args[1];

				if (dbName.Length == 0)
					return CommandResultCode.SyntaxError;

				if (String.Compare(args[2], "user", true) != 0)
					return CommandResultCode.SyntaxError;

				string adminUser = args[3];

				if (adminUser.Length == 0)
					return CommandResultCode.SyntaxError;

				string adminPass = null;

				int argIndex = 4;

				if (args.Length > 4) {
					if (String.Compare(args[4], "identified", true) == 0) {
						if (String.Compare(args[5], "by", true) != 0)
							return CommandResultCode.SyntaxError;

						adminPass = args[6];
						argIndex = 7;
					}
				}

				Hashtable config = null;

				if (args.Length > argIndex) {
					config = new Hashtable();

					if (args.Length > argIndex) {
						for (int i = argIndex; i < args.Length; i++) {
							string var = args[i];
							int index = var.IndexOf('=');
							string varname = var.Substring(0, index);
							var = var.Substring(index + 1);

							if (String.Compare(varname, "config", true) == 0) {
								//TODO: LoadFromFile(var);
								continue;
							}

							config[varname] = var;
						}
					}
				}

				if (adminPass == null)
					adminPass = Readline.ReadPassword(adminUser + " password: ");

				try {
					CreateDatabase(config, dbName, adminUser, adminPass);
					return CommandResultCode.Success;
				} catch (Exception) {
					Application.MessageDevice.WriteLine("creation failed.");
					return CommandResultCode.ExecutionFailed;
				}
			}

			return base.Execute(context, args);
		}

		private void CreateDatabase(IDictionary config, string name, string adminUser, string adminPass) {
			DeveelDbConnectionStringBuilder connString = new DeveelDbConnectionStringBuilder();
			connString.Host = "(local)";
			connString.UserName = adminUser;
			connString.Password = adminPass;
			connString.Database = name;
			connString.Create = true;

			//TODO: set the additional parameters...

			/*
			DbSystem system = ((DeveelDBShell)Application).Controller.CreateDatabase(config, name, adminUser, adminPass);
			Application.MessageDevice.WriteLine("database created successfully.");

			DeveelDbConnection conn = (DeveelDbConnection)system.GetConnection(adminUser, adminPass);
			*/

			DeveelDbConnection conn = new DeveelDbConnection(connString.ConnectionString);

			try {
				conn.Open();
			} catch(DatabaseCreateException) {
				Application.MessageDevice.WriteLine("An error occurred while creating the database '" + name + "'.");
				return;
			}

			Application.MessageDevice.WriteLine("database created successfully.");
			SqlSession session = new SqlSession((DeveelDBShell)Application, conn);

			((DeveelDBShell)Application).SessionManager.SetCurrentSession(session);
			Application.SetPrompt(session.Name + "> ");
		}

		public override bool HandleCommandLine(CommandLine commandLine) {
			if (!commandLine.HasOption("create"))
				return false;

			string user = commandLine.GetOptionValue("user");
			string pass = commandLine.GetOptionValue("pass");
			string database = commandLine.GetOptionValue("database");

			while (user == null || user.Length == 0) {
				user = Readline.ReadLine("Username: ");
				if (user == null || user.Length == 0)
					Application.MessageDevice.WriteLine("you must specify a valid user name");
			}

			if (pass == null)
				pass = Readline.ReadPassword("Password: ");

			Hashtable config = new Hashtable();

			// Find all '-C*' style switches,
			String[] c_args = commandLine.GetOptionValues('C');
			if (c_args != null) {
				for (int i = 0; i < c_args.Length; i += 2) {
					string key = c_args[0];
					string value = c_args[1];
					config[key] = value;
				}
			}

			try {
				CreateDatabase(config, database, user, pass);
			} catch (Exception e) {
				Application.MessageDevice.WriteLine("error while creating: " + e.Message);
				return true;
			}

			// we return 'false' cause we want that after this
			// command is executed the application will stay open
			// and we will query the database created...
			return false;
		}
	}
}