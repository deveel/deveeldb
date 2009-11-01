using System;
using System.Globalization;
using System.IO;
using System.Reflection;

using Deveel.Configuration;
using Deveel.Data.Util;

namespace Deveel.Data {
	public class Program {
		private static CommandLineOptions GetOptions() {
			CommandLineOptions options = new CommandLineOptions();
			CommandLineOption option = new CommandLineOption("p", "path", true,
															 "the path, relative to the host filesystem " +
															 "to the database to repair");
			option.ArgumentName = "path";
			option.IsRequired = true;
			options.AddOption(option);

			option = new CommandLineOption("n", "name", true, "the name of the database to repair");
			option.ArgumentName = "name";
			options.AddOption(option);

			option = new CommandLineOption("?", "help", false, "prints this help");
			options.AddOption(option);

			return options;
		}

		private static object GetStoreSystem(TransactionSystem system) {
			Type type = typeof(TransactionSystem);
			PropertyInfo propertyInfo = type.GetProperty("StoreSystem");
			return propertyInfo.GetValue(system, BindingFlags.Instance | BindingFlags.NonPublic, null, null,
										 CultureInfo.InvariantCulture);
		}

		private static TableDataConglomerate ConstructConglomerate(TransactionSystem system) {
			Type type = typeof(TableDataConglomerate);
			return Activator.CreateInstance(type, BindingFlags.Instance | BindingFlags.NonPublic, null,
											new object[] { system, GetStoreSystem(system) }) as TableDataConglomerate;
		}

		public static void Main(string[] args) {
			CommandLineOptions options = GetOptions();

			ICommandLineParser parser = CommandLineParser.CreateParse(ParseStyle.Gnu);
			CommandLine cl = parser.Parse(options, args);

			if (cl.HasOption("?")) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.WriteHelp(Console.Out, Console.WindowWidth, "deveeldbrep", "", options, 12, 15, "");
				Environment.Exit(0);
			}

			string path = cl.GetOptionValue("path");
			string dbName = cl.GetOptionValue("name", "DefaultDatabase");

			TransactionSystem system = new TransactionSystem();

			try {
				ShellUserTerminal terminal = new ShellUserTerminal();

				DbConfig config = DbConfig.Default;
				config.SetValue("database_path", path);
				config.SetValue("log_path", "");
				config.SetValue("min_debug_level", "50000");
				config.SetValue("debug_logs", "disabled");
				system.Init(config);
				TableDataConglomerate conglomerate = ConstructConglomerate(system);
				// Check it.
				conglomerate.Fix(dbName, terminal);
			} catch (Exception e) {
				Console.Error.WriteLine("An error occurred while fixing the database.");
				Console.Error.WriteLine(e.Message);
				Environment.Exit(1);
			} finally {
				// Dispose the transaction system
				system.Dispose();
			}
			Environment.Exit(0);
		}
	}
}