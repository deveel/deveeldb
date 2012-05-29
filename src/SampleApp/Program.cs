using System;
using System.Data;
using System.IO;

using Deveel.Configuration;
using Deveel.Data.Control;

namespace SampleApp {
	class Program {
		private const string Header = "DeveelDB Sample Application";
		private const string AppDescription = "This is a sample application to demonstrate DeveelDB features";

		private static Options GetOptions() {
			Options options = new Options();
			Option option = new Option("n", "name", true, "The name of the database to create or open");
			option.IsRequired = true;
			options.AddOption(option);
			option = new Option("u", "user", true, "Name of the database administrator");
			option.IsRequired = true;
			options.AddOption(option);
			option = new Option("p", "password", true, "Password of the database administrator");
			option.IsRequired = true;
			options.AddOption(option);
			options.AddOption("c", "create", false, "Tells the application to create the database");
			options.AddOption("x", "exeute", true, "An sql command to execute within the databse");
			options.AddOption("f", "file", false, "Used to indicate the 'execute' argument is a file");
			options.AddOption("h", "help", false, "Prints this help");
			options.AddOption("s", "storage", true, "The storage type of the database system (heap or file)");
			options.AddOption("o", "dbpath", true, "The path to the root folder in the file-system of the storage (for storage=file)");
			return options;
		}

		static int Main(string[] args) {
			Console.Out.WriteLine(Header);
			Console.Out.WriteLine(AppDescription);
			Console.Out.WriteLine();

			Options options = GetOptions();
			GnuParser parser = new GnuParser(options);
			CommandLine commandLine;

			try {
				commandLine = parser.Parse(args, false);
			} catch (MissingOptionException e) {
				string[] missingOptions = new string[e.MissingOptions.Count];
				for (int i = 0; i < missingOptions.Length; i++) {
					Option option = options.GetOption((string)e.MissingOptions[i]);
					missingOptions[i] = option.HasLongName ? option.LongName : option.Name;
				}

				Console.Error.WriteLine("Missing required option: {0}", String.Join(", ", missingOptions));
				return 1;
			}
			

			if (!commandLine.HasParsed) {
				HelpFormatter helpFormatter = new HelpFormatter();
				helpFormatter.Options = options;
				helpFormatter.PrintUsage(Console.Out);
				return 1;
			}

			if (commandLine.HasOption("h")) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.Options = options;
				formatter.PrintHelp(Console.Out, true);
				return 0;
			}

			string dbName = commandLine.GetOptionValue("n");
			string user = commandLine.GetOptionValue("u");
			string pass = commandLine.GetOptionValue("p");

			string storageString = commandLine.GetOptionValue("s", "file");

			DbConfig config = new DbConfig();
			if (String.Equals(storageString, "file", StringComparison.InvariantCultureIgnoreCase)) {
				Console.Out.WriteLine("Using file-system based storage");
				config.SetValue(ConfigKeys.StorageSystem, ConfigValues.FileStorageSystem);

				string dbPath = commandLine.GetOptionValue("o");
				config.SetValue(ConfigKeys.DatabasePath, dbPath);
			} else if (String.Equals(storageString, "heap", StringComparison.InvariantCultureIgnoreCase)) {
				Console.Out.WriteLine("Using heap based storage");
				config.SetValue(ConfigKeys.StorageSystem, ConfigValues.HeapStorageSystem);
			} else {
				Console.Out.WriteLine("Invalid storage type '{0}' set: falling back to 'file'", storageString);
				
				config.SetValue(ConfigKeys.StorageSystem, ConfigValues.FileStorageSystem);
			}

			config.SetValue(ConfigKeys.BasePath, "./db");

			//let's log something up
			config.SetValue(ConfigKeys.DebugLogs, true);
			config.SetValue(ConfigKeys.LogPath, "./logs");
			config.SetValue(ConfigKeys.DebugLogFile, String.Format("{0}.log", dbName));
			config.SetValue(ConfigKeys.DebugLevel, 0);

			DbController controller = DbController.Create(config);

			DbSystem system;
			if (commandLine.HasOption("c")) {
				Console.Out.WriteLine("Creating database {0} with admin {1}", dbName, user);
				system = controller.CreateDatabase(config, dbName, user, pass);
			} else {
				if (!controller.DatabaseExists(dbName)) {
					Console.Out.WriteLine("Database {0} doesn't exist: creating it now", dbName);

					system = controller.CreateDatabase(config, dbName, user, pass);
				} else {
					Console.Out.WriteLine("Opening database {0}", dbName);
					system = controller.StartDatabase(config, dbName);
				}
			}

			Console.Out.WriteLine();
			Console.Out.WriteLine("Getting connection for user {0} to the database", user);

			IDbConnection connection = system.GetConnection(user, pass);

			try {
				int commandCount = 0;
				TimeSpan elapsed = TimeSpan.Zero;

				if (commandLine.HasOption("x")) {
					string toExecute = commandLine.GetOptionValue("x");

					TextReader reader;
					if (commandLine.HasOption("f")) {
						Console.Out.WriteLine("Reading commands to execute from file");

						reader = new StreamReader(toExecute);
					} else {
						reader = new StringReader(toExecute);
					}

					DateTime startTime = DateTime.Now;

					string line;
					while ((line = reader.ReadLine()) != null) {
						Console.Out.WriteLine("Executing: {0}", line);
						IDbCommand command = connection.CreateCommand();
						command.CommandText = line;
						command.CommandType = CommandType.Text;
						int affected = command.ExecuteNonQuery();

						Console.Out.WriteLine("    {0} rows affected", affected);
						Console.Out.WriteLine();

						commandCount++;
					}

					elapsed = DateTime.Now.Subtract(startTime);
				}

				Console.Out.WriteLine("  {0} commands executed in {1}ms", commandCount, Math.Round(elapsed.TotalMilliseconds, 3, MidpointRounding.ToEven));
				Console.Out.WriteLine("Closing...");
				connection.Close();

				return 0;
			} catch (Exception e) {
				Console.Out.WriteLine("Error while executing command...");
				Console.Error.WriteLine(e.Message);
				Console.Error.WriteLine(e.StackTrace);
				return 1;
			} finally {
				connection.Dispose();
				system.Dispose();
			}
		}
	}
}
