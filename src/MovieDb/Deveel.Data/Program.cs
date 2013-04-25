using System;
using System.Data;

using Deveel.Configuration;
using Deveel.Data.Control;

namespace Deveel.Data {
	class Program {
		private static DbSystem dbSystem;

		private const string DbName = "movies";
		private const string DbAdmin = "admin";
		private const string DbPassword = "123456";

		private static void SetupDbSystem(CommandLine commandLine) {
			var dbConfig = new DbConfig();
			if (commandLine.HasOption('s')) {
				var storage = commandLine.GetOptionValue('s');
				if (String.IsNullOrEmpty(storage) ||
				    String.Equals(storage, "file", StringComparison.InvariantCultureIgnoreCase)) {
					dbConfig.SetValue(ConfigKeys.StorageSystem, ConfigValues.FileStorageSystem);
					if (commandLine.HasOption('f'))
						dbConfig.SetValue(ConfigKeys.DatabasePath, commandLine.GetOptionValue('f'));
				} else {
					dbConfig.SetValue(ConfigKeys.StorageSystem, ConfigValues.HeapStorageSystem);
				}
			} else {
				dbConfig.SetValue(ConfigKeys.StorageSystem, ConfigValues.HeapStorageSystem);
			}

			var controller = DbController.Create(dbConfig);

			if (!controller.DatabaseExists(DbName)) {
				dbSystem = controller.CreateDatabase(dbConfig, DbName, DbAdmin, DbPassword);
				CreateDatabase();
			} else {
				dbSystem = controller.ConnectToDatabase(DbName);
			}
		}

		private static void CreateDatabase() {
			var dbConn = dbSystem.GetConnection("APP", DbAdmin, DbPassword);

			var command = dbConn.CreateCommand();
			command.CommandType = CommandType.Text;
			command.CommandText = "CREATE TABLE IF NOT EXISTS Movies (id IDENTITY, name VARCHAR, year INT)";
			command.ExecuteNonQuery();
		}

		private static Options CreateOptions() {
			Options options = new Options();
			options.AddOption("s", "storage", true, "The storage type to use for the database.");
			options.AddOption("f", "file", true, "In case of a 'file' storage system, this is the path to the database.");
			OptionGroup commandGroup = new OptionGroup();
			commandGroup.AddOption(new Option("a", "add", true, "Adds a new title to the database."));
			commandGroup.AddOption(new Option("q", "query", true, "Queries the database for a given movie."));
			commandGroup.AddOption(new Option("t", "title", true, "Adds a title to the movie by the id"));
			options.AddOptionGroup(commandGroup);
			options.AddOption("x", "exit", false, "Exits the application.");
			options.AddOption("h", "help", false, "Shows the help");
			return options;
		}

		static void Main(string[] args) {
			var options = CreateOptions();

			var parser = Parser.Create(ParseStyle.Gnu);
			parser.Options = options;
			var commandLine = parser.Parse(args);

			SetupDbSystem(commandLine);

			while (!commandLine.HasOption('x')) {
				if (commandLine.HasOption('a')) {
					AddMovie(commandLine);
				} else if (commandLine.HasOption('q')) {
					QueryTitle(commandLine);
				} else if (commandLine.HasOption('t')) {
					AddTitle(commandLine);
				}

				Console.Out.WriteLine("Type a new command (-x or -exit to quit)");
				Console.Out.WriteLine();

				string line = Console.In.ReadLine();
				if (String.IsNullOrEmpty(line))
					break;

				args = line.Split(' ');
				parser.Options = options;
				commandLine = parser.Parse(args);
			}

			Console.Out.WriteLine("Good bye!");
		}

		private static void AddTitle(CommandLine commandLine) {
			var dbConn = dbSystem.GetConnection("APP", DbAdmin, DbPassword);
		}

		private static void QueryTitle(CommandLine commandLine) {
			var dbConn = dbSystem.GetConnection("APP", DbAdmin, DbPassword);
		}

		private static void AddMovie(CommandLine commandLine) {
			var dbConn = dbSystem.GetConnection("APP", DbAdmin, DbPassword);
		}
	}
}
