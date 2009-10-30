//  
//  Program.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.
//

using System;
using System.Collections;
using System.Collections.Specialized;
using System.Configuration.Install;
using System.Data;
using System.IO;
using System.ServiceProcess;

using Deveel.Configuration;
using Deveel.Data.Client;
using Deveel.Data.Control;

namespace Deveel.Data.Server {
	public sealed class Program {
		private static CommandLineOptions GetOptions() {
			CommandLineOptions options = new CommandLineOptions();
			CommandLineOption option;

			option = new CommandLineOption("?", "help", false, "Prints this help.");
			options.AddOption(option);

			option = new CommandLineOption("c", "conf", true,
			                               "The database configuration file to use.  If not specified then it searches " +
			                               "for 'db.conf' in the current directory.");
			option.ArgumentName = "config-file";
			options.AddOption(option);

			option = new CommandLineOption("dbpath", true, "Specifies where the database data files are located.");
			option.ArgumentName = "path";
			options.AddOption(option);

			option = new CommandLineOption("l", "logpath", true, "Specifies where the logs are to be kept.");
			option.ArgumentName = "logpath";
			options.AddOption(option);

			option = new CommandLineOption("h", "address", true,
			                               "For multi-homed machines, allows for the database to bind to a particular " +
			                               "host address.");
			option.ArgumentName = "host";
			options.AddOption(option);

			option = new CommandLineOption("u", "user", true, "The name of the user that computes the operation.");
			option.ArgumentName = "name";
			options.AddOption(option);

			option = new CommandLineOption("p", "password", true,
			                               "The password used to identify the user. When not " +
			                               "provided explicitily, the application will ask to provide it in a masked way.");
			option.HasOptionalArgument = true;
			options.AddOption(option);

			option = new CommandLineOption("port", true, "Sets the TCP port where the clients must connect to.");
			option.ArgumentName = "port";
			option.Type = typeof (int);
			options.AddOption(option);

			option = new CommandLineOption("d", "database", true, "The name of the database to boot or create.");
			option.ArgumentName = "name";
			options.AddOption(option);

			option = new CommandLineOption("C", true,
			                               "Where <key> is a configuration property and <value> is a value to set the " +
			                               "property to. This can be used to override any property in the configuration " +
			                               "file.  Example: -Cmaximum_worker_threads=2");
			option.ArgumentCount = 2;
			option.ArgumentName = "key=value";
			option.ValueSeparator = '=';
			options.AddOption(option);

			CommandLineOptionGroup commandsGroup = new CommandLineOptionGroup();

			option = new CommandLineOption("n", "create", false,
			                               "Creates an empty database and adds a user with the given username and " +
			                               "password with complete privs. This will not start the database server.");
			commandsGroup.AddOption(option);

			option = new CommandLineOption("s", "shutdown", false,
			                               "Shuts down the database server running on the host/port. [host] and " +
			                               "[port] are optional, they default to 'localhost' and port 9157.");
			commandsGroup.AddOption(option);

			option = new CommandLineOption("b", "boot", false,
			                               "Boots the database server from the information given in the configuration " +
			                               "file. This switch is implied if no other function switch is provided.");
			commandsGroup.AddOption(option);

			option = new CommandLineOption("i", "install", false,
			                               "Installs the TCP/IP service in the current machine applying the given arguments " +
			                               "to the installation process. All the configurations concerning the system " +
			                               "will be recovered from a default configuration file in the same directory of " +
			                               "the executable.");
			commandsGroup.AddOption(option);

			option = new CommandLineOption("r", "uninstall", false,
			                               "If the TCP/IP service was previously installed in the current machine, this " +
			                               "command uninstalls it definitively.");
			commandsGroup.AddOption(option);

			options.AddOptionGroup(commandsGroup);

			return options;
		}

		/**
		 * Performs the create command.
		 */
		private static void doCreate(String database_name, String username, String password, DbConfig config) {
			DbController controller = DbController.Default;
			// Create the database with the given configuration then Close it
			if (!controller.DatabaseExists(database_name)) {
				DbSystem database = controller.CreateDatabase(config, database_name, username, password);
				database.Close();
			}
		}

		/// <summary>
		/// Performs the shutdown command.
		/// </summary>
		/// <param name="host"></param>
		/// <param name="port"></param>
		/// <param name="database"></param>
		/// <param name="username"></param>
		/// <param name="password"></param>
		private static void doShutDown(String host, String port, string database, String username, String password) {

			// Actually - config bundle useless for this....
			DeveelDbConnection connection;
			try {
				ConnectionString connectionString = new ConnectionString();
				connectionString.Host = host;
				connectionString.Port = Int32.Parse(port);
				connectionString.UserName = username;
				connectionString.Password = password;
				connectionString.Database = database;
				connection = new DeveelDbConnection(connectionString.ToString());
				connection.Open();
			} catch (Exception e) {
				Console.Error.WriteLine(e.Message);
				Console.Error.WriteLine(e.StackTrace);
				throw;
			}

			try {
				DeveelDbCommand statement = connection.CreateCommand("SHUTDOWN");
				statement.ExecuteNonQuery();
			} catch (DataException e) {
				Console.Out.WriteLine("Unable to shutdown database: " + e.Message);
				throw;
			}

			try {
				connection.Close();
			} catch (DataException e) {
				Console.Out.WriteLine("Unable to Close connection: " + e.Message);
				throw;
			}

		}

		private static void InstallService(string user, string pass, string[] args) {
			// Initialize the installer for the assembly containing the TCP/IP service...
			AssemblyInstaller installer = new AssemblyInstaller(typeof(TcpService).Assembly, args);
			installer.UseNewContext = true;
			// ... install the service (this will call the TcpServiceInstaller class) ...
			ListDictionary savedState = new ListDictionary();
			savedState["user"] = user;
			savedState["pass"] = pass;
			installer.Install(savedState);
			// ... and commit everything.
			installer.Commit(savedState);
		}

		private static void UninstallService(string user, string pass, string[] args) {
			AssemblyInstaller installer = new AssemblyInstaller(typeof(TcpService).Assembly, args);
			Hashtable savedState = new Hashtable();
			savedState["user"] = user;
			savedState["pass"] = pass;
			installer.UseNewContext = false;
			installer.Uninstall(savedState);
			installer.Commit(savedState);
		}

		/// <summary>
		/// Performs the boot command.
		/// </summary>
		/// <param name="conf"></param>
		private static void doBoot(string path, IDbConfig config) {
			// Connect a TcpServerController to it.
			TcpServerController serverController = new TcpServerController(DbController.Create(path, config));
			// And start the server
			serverController.Start();

			// Output a message telling us about the server
			Console.Out.Write(serverController.ToString());
			Console.Out.WriteLine(".");
		}

		public static void Main(string[] args) {
			CommandLineOptions options = GetOptions();
			Configuration.CommandLine commandLine = CommandLineParser.Parse(ParseStyle.Gnu, options, args, true);
			if (commandLine.HasOption("help")) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.Width = Console.WindowWidth;
				formatter.WriteHelp("deveeldbd", null, options, null, true);
				// formatter.WriteOptions(Console.Out, 80, options, 10, 2);
				Environment.Exit(0);
			}

			// Print the startup message,
			Console.Out.WriteLine();
			Console.Out.WriteLine(ProductInfo.Current.Title + ProductInfo.Current.Version);
			Console.Out.WriteLine(ProductInfo.Current.Copyright);
			Console.Out.WriteLine("Use: -help for printing the usage information.");

			Console.Out.WriteLine();
			Console.Out.WriteLine("  DeveelDB comes with ABSOLUTELY NO WARRANTY.");
			Console.Out.WriteLine("  This is free software, and you are welcome to redistribute");
			Console.Out.WriteLine("  it under certain conditions. See COPYING for details of the");
			Console.Out.WriteLine("  GPLv3 License.");

			string username = commandLine.GetOptionValue("user");
			string password = commandLine.GetOptionValue("password");
			string host = commandLine.GetOptionValue("address", "localhost");
			string port = commandLine.GetOptionValue("port", "9157");
			string database = commandLine.GetOptionValue("database");

			if (password == null && commandLine.HasOption('p')) {
				// TODO: prompt a line to read the password as hidden...
			}

			if (commandLine.HasOption("shutdown")) {
				// -shutdown [host] [port] [admin_username] [admin_password]
				// Try to match the shutdown switch.

				doShutDown(host, port, database, username, password);
				Environment.Exit(0);
				return;
			}

			if (commandLine.HasOption("install")) {
				// the user is trying to install the service into the machine...

				string[] installArgs = commandLine.GetOptionValues("install");
				string[] addArgs = commandLine.GetOptionValues("C");

				try {
					if (installArgs != null && installArgs.Length > 0) {
						InstallService(installArgs[0], installArgs[1], addArgs);
					} else {
						InstallService(null, null, addArgs);
					}
					Environment.Exit(0);
					return;
				} catch (Exception e) {
					Console.Out.WriteLine("Error while installing the service: {0}", e.Message);
					Console.Out.WriteLine();
					Environment.Exit(1);
				}
			}

			if (commandLine.HasOption("uninstall")) {
				string[] installArgs = commandLine.GetOptionValues("install");
				string[] addArgs = commandLine.GetOptionValues("C");

				try {
					UninstallService(installArgs[0], installArgs[1], addArgs);
					Environment.Exit(0);
				} catch (Exception e) {
					Console.Out.WriteLine("Error while uninstalling the service: {0}", e.Message);
					Console.Out.WriteLine();
					Environment.Exit(1);
				}
			}

			// Get the conf file if applicable.
			String conf_file = commandLine.GetOptionValue("conf", "./db.conf");

			// Extract the root part of the configuration path.  This will be the root
			// directory.
			string absolute_config_path = Path.GetFullPath(conf_file);
			string root_path = Path.GetDirectoryName(absolute_config_path);
			// Create a default DBConfig object
			DefaultDbConfig config = new DefaultDbConfig(root_path);
			try {
				if (File.Exists(conf_file))
					config.LoadFromFile(conf_file);
			} catch (IOException e) {
				Console.Out.WriteLine("Error loading configuration file '" + conf_file + "': " + e.Message);
				Console.Out.WriteLine();
				Environment.Exit(1);
			}

			// Any configuration overwritten switches?
			string cparam = commandLine.GetOptionValue("dbpath");
			if (cparam != null) {
				config.SetValue("database_path", cparam);
			}
			cparam = commandLine.GetOptionValue("logpath");
			if (cparam != null) {
				config.SetValue("log_path", cparam);
			}
			cparam = commandLine.GetOptionValue("address");
			if (cparam != null) {
				config.SetValue("server_address", cparam);
			}
			cparam = commandLine.GetOptionValue("port");
			if (cparam != null) {
				config.SetValue("server_port", cparam);
			}
			// Find all '-C*' style switches,
			String[] c_args = commandLine.GetOptionValues('C');
			if (c_args != null) {
				for (int i = 0; i < c_args.Length; i+=2) {
					string key = c_args[0];
					string value = c_args[1];
					config.SetValue(key, value);
				}
			}

			// Try to match create switch.
			if (commandLine.HasOption("create")) {
				try {
					doCreate(database, username, password, config);
					Environment.Exit(0);
				} catch(Exception e) {
					Console.Error.WriteLine("An error occurred while creating the database.");
					Console.Error.WriteLine(e.Message);
					Environment.Exit(1);
				}
			}

			// Log the start time.
			DateTime start_time = DateTime.Now;

			// Nothing matches, so we must be wanting to boot a new server...
			doBoot(commandLine.GetOptionValue("dbpath", Environment.CurrentDirectory), config);

			TimeSpan count_time = DateTime.Now - start_time;
			Console.Out.WriteLine("Boot time: " + count_time.TotalMilliseconds + "ms.");

			Console.Out.WriteLine();
			Console.Out.WriteLine("Press any key to stop...");
			Console.In.ReadLine();
			Environment.Exit(0);
		}
	}
}