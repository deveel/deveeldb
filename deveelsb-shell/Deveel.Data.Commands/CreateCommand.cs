using System;

using Deveel.Commands;
using Deveel.Configuration;
using Deveel.Data.Client;
using Deveel.Data.Control;
using Deveel.Data.Shell;

namespace Deveel.Data.Commands {
	[Command("create", ShortDescription = "creates a database or a database object.")]
	[CommandSynopsis("create database <name> user <user> [ identified by <password> ] [ <var>=<value> ... ]")]
	[CommandSynopsis("create table | view | trigger ... ")]
	class CreateCommand : SqlCommand {
		// we disable the context for this command, since we want to 
		// enable the function of creating a database...
		public override bool RequiresContext {
			get { return false; }
		}

		protected override bool IsUpdateCommand {
			get { return true; }
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

				DbConfig config = null;
				string dbPath = null;

				if (args.Length > argIndex) {
					dbPath = args[argIndex];
					if (dbPath.IndexOf('=') != -1) {
						argIndex = 8;
						dbPath = null;
					}

					config = new DbConfig(args[2]);

					if (args.Length > argIndex) {
						for (int i = argIndex; i < args.Length; i++) {
							string var = args[i];
							int index = var.IndexOf('=');
							string varname = var.Substring(0, index);
							var = var.Substring(index + 1);

							if (String.Compare(varname, "config", true) == 0) {
								config.LoadFromFile(var);
								continue;
							}

							config.SetValue(varname, var);
						}
					}
				}

				if (adminPass == null)
					adminPass = Readline.ReadPassword(adminUser + " password: ");

				try {
					DbController controller = dbPath != null ? DbController.Create(dbPath) : DbController.Default;
					DbSystem system = controller.CreateDatabase(config, dbName, adminUser, adminPass);
					Application.MessageDevice.WriteLine("database created successfully.");

					DeveelDbConnection conn = (DeveelDbConnection) system.GetConnection(adminUser, adminPass);
					SqlSession session = new SqlSession((DeveelDBShell) Application, conn);

					((DeveelDBShell) Application).SessionManager.SetCurrentSession(session);
					Application.SetPrompt(session.Name + "> ");

					return CommandResultCode.Success;
				} catch (Exception) {
					Application.MessageDevice.WriteLine("creation failed.");
					return CommandResultCode.ExecutionFailed;
				}
			}

			return base.Execute(context, args);
		}

		public override void HandleCommandLine(CommandLine commandLine) {
			if (!commandLine.HasOption("create"))
				return;

			string user = commandLine.GetOptionValue("user");
			string pass = commandLine.GetOptionValue("pass");
			string database = commandLine.GetOptionValue("database");

			if (pass == null)
				pass = Readline.ReadPassword(user + " password: ");

			DbConfig config = new DbConfig(null);

			// Find all '-C*' style switches,
			String[] c_args = commandLine.GetOptionValues('C');
			if (c_args != null) {
				for (int i = 0; i < c_args.Length; i += 2) {
					string key = c_args[0];
					string value = c_args[1];
					config.SetValue(key, value);
				}
			}
		}
	}
}