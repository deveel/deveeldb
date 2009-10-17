//  
//  DbController.cs
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

using System;

using Deveel.Diagnostics;

namespace Deveel.Data.Control {
	/// <summary>
	/// An object that provides methods for creating and controlling database
	/// systems in the current environment.
	/// </summary>
	public sealed class DbController {
		private static readonly DbController DefaultController = new DbController();

		/// <summary>
		/// This object can not be constructed publicaly.
		/// </summary>
		internal DbController() {
		}

		private const string DefaultDatabaseName = "DefaultDatabase";

		/// <summary>
		/// Returns the static controller for this environment.
		/// </summary>
		public static DbController Default {
			get { return DefaultController; }
		}

		///<summary>
		/// Checks if a database exists in the given directory of the file system, 
		/// otherwise returns false if the path doesn't contain a database.
		///</summary>
		///<param name="config">The configuration of the database to check the 
		/// existence of.</param>
		/// <param name="name">The name of the database.</param>
		/// <remarks>
		///  The path string must be formatted using Unix '/' deliminators as
		/// directory separators.
		/// </remarks>
		///<returns>
		/// Returns true if a database exists at the given path, false otherwise.
		/// </returns>
		public bool DatabaseExists(IDbConfig config, string name) {
			Database database = CreateDatabase(config, name);
			bool b = database.Exists;
			database.System.Dispose();
			return b;
		}

		///<summary>
		/// Checks if a database exists in the given directory of the file system, 
		/// otherwise returns false if the path doesn't contain a database.
		///</summary>
		///<param name="config">The configuration of the database to check the 
		/// existence of.</param>
		/// <remarks>
		///  The path string must be formatted using Unix '/' deliminators as
		/// directory separators.
		/// </remarks>
		///<returns>
		/// Returns true if a database exists at the given path, false otherwise.
		/// </returns>
		public bool DatabaseExists(IDbConfig config) {
			string name = config.GetValue("name");
			if (name == null)
				name = DefaultDatabaseName;
			return DatabaseExists(config, name);
		}

		///<summary>
		/// Creates a database in the local runtime (and filesystem) given the
		/// configuration in IDbConfig and returns a DbSystem object.
		///</summary>
		///<param name="config">The configuration of the database to create and start 
		/// in the local runtime.</param>
		///<param name="admin_user">The username of the administrator for the new 
		/// database.</param>
		///<param name="admin_pass">The password of the administrator for the new 
		/// database.</param>
		/// <remarks>
		/// When this method returns, the database created will be up and running 
		/// providing there was no failure during the database creation process.
		/// </remarks>
		///<returns>
		/// Returns the <see cref="DbSystem"/> object used to access the database created.
		/// </returns>
		///<exception cref="Exception">
		/// Thrown if the database path does not exist.
		/// </exception>
		public DbSystem CreateDatabase(IDbConfig config, String admin_user, String admin_pass) {
			string name = config.GetValue("name");
			if (name == null)
				name = DefaultDatabaseName;
			return CreateDatabase(config, name, admin_user, admin_pass);
		}

		public DbSystem CreateDatabase(IDbConfig config, string name, String admin_user, String admin_pass) {
			// Create the Database object with this configuration.
			Database database = CreateDatabase(config, name);
			DatabaseSystem system = database.System;

			// Create the database.
			try {
				database.Create(admin_user, admin_pass);
				database.Init();
			} catch (DatabaseException e) {
				Debug.Write(DebugLevel.Error, this, "Database create failed");
				Debug.WriteException(e);
				throw new Exception(e.Message);
			}

			// Return the DbSystem object for the newly created database.
			return new DbSystem(this, config, database);
		}

		///<summary>
		/// Starts a database in the local runtime given the configuration in 
		/// <see cref="IDbConfig"/> and returns a <see cref="DbSystem"/> object.
		///</summary>
		///<param name="config">The configuration of the database to start in 
		/// the local runtime.</param>
		/// <remarks>
		/// When this method returns, the database will be up and running providing 
		/// there was no failure to initialize the database.
		/// </remarks>
		///<returns>
		/// Returns the <see cref="DbSystem"/> object used to access the database started.
		/// </returns>
		///<exception cref="Exception">
		/// Thrown if the database does not exist in the path given in the configuration.
		/// </exception>
		public DbSystem StartDatabase(IDbConfig config) {
			string name = config.GetValue("name");
			if (name == null)
				name = DefaultDatabaseName;
			return StartDatabase(config, name);
		}

		public DbSystem StartDatabase(IDbConfig config, string name) {
			// Create the Database object with this configuration.
			Database database = CreateDatabase(config, name);
			DatabaseSystem system = database.System;

			// First initialise the database
			try {
				database.Init();
			} catch (DatabaseException e) {
				Debug.Write(DebugLevel.Error, this, "Database init failed");
				Debug.WriteException(e);
				throw new Exception(e.Message);
			}

			// Return the DbSystem object for the newly created database.
			return new DbSystem(this, config, database);
		}


		// ---------- Static methods ----------

		/// <summary>
		/// Creates a Database object for the given IDbConfig configuration.
		/// </summary>
		/// <param name="config"></param>
		/// <returns></returns>
		private static Database CreateDatabase(IDbConfig config, string name) {
			DatabaseSystem system = new DatabaseSystem();

			// Initialize the DatabaseSystem first,
			// ------------------------------------

			// This will throw an Error exception if the database system has already
			// been initialized.
			system.Init(config);

			// Start the database class
			// ------------------------

			// Note, currently we only register one database, and it is named
			//   'DefaultDatabase'.
			//TODO: check ...
			// Database database = new Database(system, "DefaultDatabase");
			Database database = new Database(system, name);

			// Start up message
			Debug.Write(DebugLevel.Message, typeof(DbController), "Starting Database Server");

			return database;
		}
	}
}