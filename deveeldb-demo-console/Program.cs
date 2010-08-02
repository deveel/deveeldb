using System;
using System.Data;

using Deveel.Data.Control;
using Deveel.Configuration;

namespace Deveel.Data {
	class Program {
		private static CommandLineOptions GetOptions() {
			CommandLineOptions options = new CommandLineOptions();
			options.AddOption("base", true, "The base used to resolve the application paths.");
			options.AddOption("?", "help", false, "Prints the help of this application.");
			options.AddOption("u", "user", true, "The name of the user (when creating a database, " + 
			                  "this will be the name of the system administrator)");
			options.AddOption("p", "pass", true, "Password used to identify the user (when creating a "+
			                  "database, this will be the system administration password)");
			options.AddOption("command", true, "Command to execute within the application (either 'create', 'update', 'select' or 'delete')");
			return options;
		}
		
		private static DbConfig CreateConfig(CommandLineOptions options) {
			DbConfig config = DbConfig.Default;
			
			string value = options.GetOption("base");
			if (!String.IsNullOrEmpty(value))
				config.CurrentPath = value;
			
			return config;
		}
		
		private static bool DbExists(IDbConnection connection) {
			return false;
		}
		
		private static bool CreateDb(IDbConnection connection) {
			using(IDbCommand command = connection.CreateCommand()) {
				command.CommandText = "CREATE TABLE person_info ( " + 
					"id IDENTITY, " +
					"first_name VARCHAR(100) NOT NULL, " +
					"last_name VARCHAR(100) NOT NULL, " +
					")";
				
				command.ExecuteNonQuery();
				return true;
			}
			return false;
		}
		
		private bool CreatePerson(IDbConnection connection) {
			PersonInfo person = new PersonInfo();
			
			// first name and last name are the required information to
			// check if we already have a record ...
			while (String.IsNullOrEmpty(person.FirstName = Readline.ReadLine("First Name: ")))
				Console.Out.WriteLine("Please provide a valid first name ...");
			while(String.IsNullOrEmpty(person.LastName = Readline.ReadLine("Last Name: ")))
				Console.Out.WriteLine("Please provide a valid last name ...");
			
			bool insert;
			if (PersonExists(person.FirstName, person.LastName, out insert) && !insert) {
				Console.Out.WriteLine("{0} {1} already exists and we-re not continuing inserting.")
				return false;
			}
		}
		
		private bool PersonExists(IDbConnection connection, string firstName, string lastName, out bool insert) {
			int value;
			using (IDbCommand command = connection.CreateCommand()) {
				command.CommandText = "SELECT COUNT(*) FROM person_info WHERE first_name = '" + firstName + "' AND last_name = '" + lastName + "'";
				
				value = (int) command.ExecuteScalar();
			}
			
			if (value > 0) {
				string answer;
				while (String.IsNullOrEmpty(answer = Readline.ReadLine("{0} {1} already exists in the database: do you want to continue anyway?")))
					Console.Out.WriteLine("Please provide a valid answer ...");
				
				insert = answer.Equals("y", StringComparison.InvariantCultureIgnoreCase) ||
					answer.Equals("yes", StringComparison.InvariantCultureIgnoreCase);
				
				return true;
			}
			
			insert = false;
			return false;
		}
		
		public static int Main(string[] args) {
			// first of all, an application specific thing: the options ...
			CommandLineOptions options = GetOptions();
			ICommandLineParser parser = CommandLineParser.CreateParse(ParseStyle.Gnu);
			
			CommandLine commandLine;
			
			try {
				commandLine = parser.Parse(options, args);
			} catch(Exception e) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.WriteUsage(Console.Out, Console.WindowWidth, "", options);
				return 1;
			}
			
			if (options.HasOption("?")) {
				HelpFormatter formatter = new HelpFormatter();
				formatter.WriteHelp(Console.Out, Console.WindowWidth, "", "", options, 4, 4, "");
				return 0;
			}
			
			// if specified, get the database name
			string dbName = options.GetOption("db");
			if (String.IsNullOrEmpty(dbName)) {
				// otherwise set the default one for this test
				Console.Out.WriteLine("The database name was not assigned: setting to default ('demo_db')");
				dbName = "demo_db";
			}
			
			// if specified, get the user name
			string user = options.GetOption("user");
			if (String.IsNullOrEmpty(user)) {
				// otherwise set the default one for this test
				Console.Out.WriteLine("The user name was not assigned: setting to default ('SA')");
				user = "SA";
			}
			
			// if specified, get the user password
			string pass = options.GetOption("pass");
			if (String.IsNullOrEmpty(pass)) {
				// otherwise set the default one for this test
				Console.Out.WriteLine("The user password was not assigned: setting to default ('123456')");
				pass = "123456";
			}
			
			// given the options specified by the user, try to create
			// an instance of the DbConfig that encapsulates the values
			DbConfig config = CreateConfig(options);
			
			// once we have a wel set configuration object, pass it to the
			// controller to get an instance of the object that will be used
			// to locally access our database
			DbController controller = DbController.Create(config);
			
			// now we should determine if the database already exists in our
			// context (defined by the controller); this is an optional passage
			// but it's always better to avoid clashes and do it ...
			DbSystem system;
			if (!controller.DatabaseExists(dbName)) {
				// no database with the given name was found within the
				// controller context
				Console.Out.WriteLine("The application database was not found: generating one.");
				
				// the user name and password credentials are now used to set the
				// database administrator; the config object is passed for database-specific
				// configurations
				system = controller.CreateDatabase(config, dbName, user, pass);
			} else {
				// the database was found: we boot it up
				system = controller.StartDatabase(config, dbName);
			}
			
			// well ... the system is properly running and now we're ready to
			// obtain a standard ADO.NET IDbConnection to communicate
			// with the database through standard SQL commands...
			IDbConnection conn;
			
			try {
				// connect to the database providing the initial schema
				// and the credentials of the user connecting ...
				conn = system.GetConnection(schema, user, pass);
			} catch (Exception e) {
				Console.Out.WriteLine("an error occurred while obtaining a connection from the system: "+
				                      "please report it to db@deveel.com.");
				Console.Out.WriteLine(e.Message);
				Console.Out.WriteLine(e.StackTrace);
				return 1;
			}
			
			try {
			// was the database already configured (tables, constrainsts, etc?)
			if (!DbExists(conn)) {
				// it seems this is a new installation: generate the database structure ...
				if (!CreateDb(conn)) {
					// the generation was unsuccessful: return ...
					Console.Out.WriteLine("An error occurred while creating the database.");
					return 1;
				}
				
				return 1;
			}
			
			// now we're definitively ready to run ...
			
			string command = options.GetOption("command");
			while (String.IsNullOrEmpty(command = Readline.ReadLine("command > ")) &&
			       !IsValidCommand(command))
				Console.Out.WriteLine("Please provide a valid command ...");
			
			if (String.Compare(command, "create", true) == 0)
				return CreatePerson(conn) ? 0 : 1;
			else if (String.Compare(command, "select", true) == 0)
				return SelectPerson(conn) ? 0 : 1;
			else if (String.Compare(command, "delete", true) == 0)
				return DeletePerson(conn) ? 0 : 1;
			else if (String.Compare(command, "update", true) == 0)
				return UpdatePerson(conn) ? 0 : 1;
			
				return 0;
			} catch (Exception e) {
				Console.Out.WriteLine("an unhandled exception occurred: please report it to db@deveel.com.");
				Console.Out.WriteLine(e.Message);
				Console.Out.WriteLine(e.StackTrace);
				return 1;
			} finally {
				if (conn != null)
					conn.Dispose();
			}
		}
		
		private sealed class PersonInfo {
			public string FirstName;
			public string LastName;
			public string MiddleName;
			public int Gender;
			public DateTime BirthDate;
			public string BirthPlace;
			public string Address1;
			public string Address2;
			public string Zip;
			public string Country;
			public string City;
		}
	}
}