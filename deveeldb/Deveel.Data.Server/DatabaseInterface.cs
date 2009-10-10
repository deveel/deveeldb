//  
//  DatabaseInterface.cs
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
using System.Text;

using Deveel.Data.Client;
using Deveel.Diagnostics;

namespace Deveel.Data.Server {
	///<summary>
	/// An implementation of <see cref="IDatabaseInterface"/> on the server-side.
	///</summary>
	/// <remarks>
	/// This receives database commands and dispatches them to the database system.
	/// This assumes that all calls to the methods here are in a <see cref="WorkerThread"/>
	/// thread.
	/// <para>
	/// <b>Note</b>: Currently, the client/server use of this object isn't multi-threaded,
	/// however the local connection could be.  Therefore, this object has been made multi-
	/// thread safe.
	/// </para>
	/// </remarks>
	public class DatabaseInterface : DatabaseInterfaceBase {
		/// <summary>
		/// Set this to true if command logging is enabled.
		/// </summary>
		private const bool COMMAND_LOGGING = true;

		/// <summary>
		/// The unique host name denoting the client that's connected.
		/// </summary>
		private readonly string host_name;

		/// <summary>
		/// Sets up the processor.
		/// </summary>
		/// <param name="database"></param>
		/// <param name="host_name"></param>
		public DatabaseInterface(Database database, String host_name)
			: base(database) {
			this.host_name = host_name;
		}

		/// <summary>
		/// Tries to authenticate the username and password against the given database.
		/// </summary>
		/// <param name="database"></param>
		/// <param name="default_schema"></param>
		/// <param name="username"></param>
		/// <param name="password"></param>
		/// <param name="database_call_back"></param>
		/// <remarks>
		/// If successful, alters the state of this object to reflect the fact the user has logged in.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if we are successful.
		/// </returns>
		private bool Authenticate(Database database, String default_schema, String username, String password,
									 IDatabaseCallBack database_call_back) {
			// If the 'user' variable is null, no one is currently logged in to this
			// connection.

			if (User == null) {

				if (COMMAND_LOGGING && database.System.LogQueries) {
					// Output the instruction to the commands log.
					StringBuilder log_str = new StringBuilder();
					log_str.Append("[ADO.NET] [");
					log_str.Append(username);
					log_str.Append("] ");
					log_str.Append('[');
					log_str.Append(host_name);
					log_str.Append("] ");
					log_str.Append("Log in.\n");
					database.CommandsLog.Write(log_str.ToString());
				}

				// Write debug message,
				if (Debug.IsInterestedIn(DebugLevel.Information)) {
					Debug.Write(DebugLevel.Information, this, "Authenticate User: " + username);
				}

				// Create a UserCallBack class.
				DatabaseConnection.CallBack call_back = new CallBackImpl(database_call_back);

				// Try to create a User object.
				User this_user = database.AuthenticateUser(username, password,
														   host_name);
				DatabaseConnection database_connection = null;

				// If successful, ask the engine for a DatabaseConnection object.
				if (this_user != null) {
					database_connection =
									   database.CreateNewConnection(this_user, call_back);

					// Put the connection in exclusive mode
					LockingMechanism locker = database_connection.LockingMechanism;
					locker.SetMode(LockingMode.EXCLUSIVE_MODE);
					try {

						// By default, JDBC connections are auto-commit
						database_connection.AutoCommit = true;

						// Set the default schema for this connection if it exists
						if (database_connection.SchemaExists(default_schema)) {
							database_connection.SetDefaultSchema(default_schema);
						} else {
							Debug.Write(DebugLevel.Warning, this,
									  "Couldn't change to '" + default_schema + "' schema.");
							// If we can't change to the schema then change to the APP schema
							database_connection.SetDefaultSchema("APP");
						}

					} finally {
						try {
							// Make sure we commit the connection.
							database_connection.Commit();
						} catch (TransactionException e) {
							// Just issue a warning...
							Debug.WriteException(DebugLevel.Warning, e);
						} finally {
							// Guarentee that we unluck from EXCLUSIVE
							locker.FinishMode(LockingMode.EXCLUSIVE_MODE);
						}
					}

				}

				// If we have a user object, then init the object,
				if (this_user != null) {
					Init(this_user, database_connection);
					return true;
				} else {
					// Otherwise, return false.
					return false;
				}

			} else {
				throw new Exception("Attempt to authenticate user twice");
			}

		}

		private class CallBackImpl : DatabaseConnection.CallBack {
			public CallBackImpl(IDatabaseCallBack callBack) {
				database_call_back = callBack;
			}

			private IDatabaseCallBack database_call_back;

			public void TriggerNotify(String trigger_name, TriggerEventType trigger_event,
									  String trigger_source, int fire_count) {
				StringBuilder message = new StringBuilder();
				message.Append(trigger_name);
				message.Append(' ');
				message.Append(trigger_source);
				message.Append(' ');
				message.Append(fire_count);

				database_call_back.OnDatabaseEvent(99, message.ToString());
			}
		}

		// ---------- Implemented from IDatabaseInterface ----------

		public override bool Login(String default_schema, String username, String password,
							 IDatabaseCallBack database_call_back) {

			Database database = Database;

			return Authenticate(database, default_schema, username, password,
			                    database_call_back);
		}

		public override IQueryResponse ExecuteQuery(SqlCommand command) {

			// Check the interface isn't disposed (connection was closed).
			CheckNotDisposed();

			User user = User;
			DatabaseConnection database_connection = DatabaseConnection;

			// Log this command if command logging is enabled
			if (COMMAND_LOGGING && Database.System.LogQueries) {
				// Output the instruction to the commands log.
				StringBuilder log_str = new StringBuilder();
				log_str.Append("[JDBC] [");
				log_str.Append(user.UserName);
				log_str.Append("] ");
				log_str.Append('[');
				log_str.Append(host_name);
				log_str.Append("] ");
				log_str.Append("Query: ");
				log_str.Append(command.Text);
				log_str.Append('\n');
				user.Database.CommandsLog.Write(log_str.ToString());
			}

			// Write debug message (Information level)
			if (Debug.IsInterestedIn(DebugLevel.Information)) {
				Debug.Write(DebugLevel.Information, this, "Query From User: " + user.UserName + "@" + host_name);
				Debug.Write(DebugLevel.Information, this, "Query: " + command.Text.Trim());
			}

			// Get the locking mechanism.
			LockingMechanism locker = database_connection.LockingMechanism;
			LockingMode lock_mode = LockingMode.NONE;
			IQueryResponse response = null;
			try {
				try {

					// For simplicity - all database locking is now exclusive inside
					// a transaction.  This means it is not possible to execute
					// queries concurrently inside a transaction.  However, we are
					// still able to execute queries concurrently from different
					// connections.
					//
					// It's debatable whether we even need to perform this Lock anymore
					// because we could change the contract of this method so that
					// it is not thread safe.  This would require that the callee ensures
					// more than one thread can not execute queries on the connection.
					lock_mode = LockingMode.EXCLUSIVE_MODE;
					locker.SetMode(lock_mode);

					// Execute the command (behaviour for this comes from super).
					response = base.ExecuteQuery(command);

					// Return the result.
					return response;

				} finally {
					try {
						// This is executed no matter what happens.  Very important we
						// unlock the tables.
						if (lock_mode != LockingMode.NONE) {
							locker.FinishMode(lock_mode);
						}
					} catch (Exception e) {
						// If this throws an exception, we should output it to the debug
						// log and screen.
						Console.Error.WriteLine(e.Message);
						Console.Error.WriteLine(e.StackTrace);
						Debug.Write(DebugLevel.Error, this, "Exception finishing locks");
						Debug.WriteException(e);
						// Note, we can't throw an error here because we may already be in
						// an exception that happened in the above 'try' block.
					}
				}

			} finally {
				// This always happens after tables are unlocked.
				// Also guarenteed to happen even if something fails.

				// If we are in auto-commit mode then commit the command here.
				// Do we auto-commit?
				if (database_connection.AutoCommit) {
					// Yes, so grab an exclusive Lock and auto-commit.
					try {
						// Lock into exclusive mode.
						locker.SetMode(LockingMode.EXCLUSIVE_MODE);
						// If an error occured then roll-back
						if (response == null) {
							// Rollback.
							database_connection.Rollback();
						} else {
							try {
								// Otherwise commit.
								database_connection.Commit();
							} catch (Exception e) {
								// Dispose this response if the commit failed.
								DisposeResult(response.ResultId);
								// And throw the SQL Exception
								throw HandleExecuteThrowable(e, command);
							}
						}
					} finally {
						locker.FinishMode(LockingMode.EXCLUSIVE_MODE);
					}
				}

			}

		}


		protected override void Dispose() {
			if (User != null) {
				DatabaseConnection database = DatabaseConnection;
				LockingMechanism locker = database.LockingMechanism;
				try {
					// Lock into exclusive mode,
					locker.SetMode(LockingMode.EXCLUSIVE_MODE);
					// Roll back any open transaction.
					database.Rollback();
				} finally {
					// Finish being in exclusive mode.
					locker.FinishMode(LockingMode.EXCLUSIVE_MODE);
					// Close the database connection object.
					database.Close();
					// Log out the user
					User.Logout();
					// Call the internal dispose method.
					InternalDispose();
				}
			}
		}
	}
}