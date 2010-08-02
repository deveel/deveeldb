using System;

using Deveel.Commands;
using Deveel.Configuration;
using Deveel.Data.Client;
using Deveel.Data.Shell;

namespace Deveel.Data.Commands {
	[Command("drop", ShortDescription = "drop an object from the database")]
	[CommandSynopsis("drop <table|view|index>")]
	[CommandSynopsis("drop database <name> user <user> [ identified by <pass> ]")]
	[OptionGroup("commands")]
	[Option("drop", false, "drops a database from the system")]
	internal class DropCommand : SqlCommand {
		public override bool RequiresContext {
			get { return false; }
		}

		public override CommandResultCode Execute(object context, string[] args) {
			int argc = args.Length;

			if (argc < 2)
				return CommandResultCode.SyntaxError;

			if (args[0] == "database") {
				string dbName = args[1];

				if (dbName == null || dbName.Length == 0)
					return CommandResultCode.SyntaxError;

				if (String.Compare(args[2], "user", true) != 0)
					return CommandResultCode.SyntaxError;

				string adminUser = args[3];

				if (adminUser.Length == 0)
					return CommandResultCode.SyntaxError;

				string adminPass = null;

				if (args.Length > 4) {
					if (String.Compare(args[4], "identified", true) == 0) {
						if (String.Compare(args[5], "by", true) != 0)
							return CommandResultCode.SyntaxError;

						adminPass = args[6];
					}
				}

				if (adminPass == null)
					adminPass = Readline.ReadPassword(adminUser + " password: ");

				try {
					DropDatabase(dbName, adminUser, adminPass);
				} catch (Exception e) {
					Application.MessageDevice.WriteLine("error while dropping database: " + e.Message);
					return CommandResultCode.ExecutionFailed;
				}
			}

			return base.Execute(context, args);
		}

		private void DropDatabase(string name, string adminUser, string adminPass) {
			DeveelDbConnectionStringBuilder connString = new DeveelDbConnectionStringBuilder();
			connString.Database = name;
			connString.UserName = adminUser;
			connString.Password = adminPass;

			/*
			DbSystem system = ((DeveelDBShell)Application).Controller.ConnectToDatabase(name);
			Application.MessageDevice.WriteLine("database created successfully.");

			DeveelDbConnection conn = (DeveelDbConnection)system.GetConnection(adminUser, adminPass);
			*/
			DeveelDbConnection conn = new DeveelDbConnection(connString.ConnectionString);
			DeveelDbCommand command = conn.CreateCommand("SHUTDOWN");
			command.ExecuteNonQuery();
			conn.Close();

			//TODO: phisically delete the database...
		}

		public override bool HandleCommandLine(CommandLine commandLine) {
			if (!commandLine.HasOption("drop"))
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

			try {
				DropDatabase(database, user, pass);
			} catch (Exception e) {
				Application.MessageDevice.WriteLine("error while creating: " + e.Message);
				return true;
			}

			return false;
		}
	}
}