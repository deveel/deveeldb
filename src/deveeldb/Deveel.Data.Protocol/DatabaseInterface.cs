// 
//  Copyright 2010-2014 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System;
using System.Text;

using Deveel.Data.Configuration;
using Deveel.Data.DbSystem;
using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Threading;
using Deveel.Data.Transactions;
using Deveel.Diagnostics;

namespace Deveel.Data.Protocol {
	///<summary>
	/// An implementation of <see cref="IDatabaseInterface"/> on the server-side.
	///</summary>
	/// <remarks>
	/// This receives database _queries and dispatches them to the database system.
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
		/// The unique host name denoting the client that's connected.
		/// </summary>
		private readonly string host_name;

		/// <summary>
		/// Sets up the processor.
		/// </summary>
		/// <param name="handler"></param>
		/// <param name="databaseName"></param>
		/// <param name="host_name"></param>
		public DatabaseInterface(IDatabaseHandler handler, string databaseName, String host_name)
			: base(handler, databaseName) {
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
		private bool Authenticate(IDatabase database, String default_schema, String username, String password, DatabaseEventCallback database_call_back) {
			// If the 'user' variable is null, no one is currently logged in to this
			// connection.

			if (User != null)
				throw new Exception("Attempt to authenticate user twice");

			if (database.Context.Logger.IsInterestedIn(LogLevel.Debug)) {
				// Output the instruction to the _queries log.
				database.Context.Logger.Debug(this, String.Format("[CLIENT] [{0}][{1}] - Log in", username, host_name));
			}

			// Write debug message,
			if (Logger.IsInterestedIn(LogLevel.Info)) {
				Logger.Info(this, "Authenticate User: " + username);
			}

			// Try to create a User object.
			User thisUser = database.AuthenticateUser(username, password, host_name);
			IDatabaseConnection database_connection = null;

			// If successful, ask the engine for a DatabaseConnection object.
			if (thisUser != null) {
				database_connection = database.CreateNewConnection(thisUser, delegate(string triggerName, string triggerSource, TriggerEventType eventType, int fireCount) {
				                                                              	StringBuilder message = new StringBuilder();
				                                                              	message.Append(Convert.ToInt32(eventType));
				                                                              	message.Append(' ');
				                                                              	message.Append(triggerName);
				                                                              	message.Append(' ');
				                                                              	message.Append(triggerSource);
				                                                              	message.Append(' ');
				                                                              	message.Append(fireCount);

				                                                              	database_call_back(99, message.ToString());

				                                                              });

				// Put the connection in exclusive mode
				LockingMechanism locker = database_connection.LockingMechanism;
				locker.SetMode(LockingMode.Exclusive);
				try {

					// By default, JDBC connections are auto-commit
					database_connection.AutoCommit = true;

					// Set the default schema for this connection if it exists
					if (database_connection.SchemaExists(default_schema)) {
						database_connection.SetDefaultSchema(default_schema);
					} else {
						Logger.Warning(this, "Couldn't change to '" + default_schema + "' schema.");
						// If we can't change to the schema then change to the APP schema
						database_connection.SetDefaultSchema("APP");
					}

				} finally {
					try {
						// Make sure we commit the connection.
						database_connection.Commit();
					} catch (TransactionException e) {
						// Just issue a warning...
						Logger.Warning(this, e);
					} finally {
						// Guarentee that we unluck from EXCLUSIVE
						locker.FinishMode(LockingMode.Exclusive);
					}
				}

			}

			// If we have a user object, then init the object,
			if (thisUser != null) {
				Init(thisUser, database_connection);
				return true;
			}

			// Otherwise, return false.
			return false;
		}

		// ---------- Implemented from IDatabaseInterface ----------

		public override bool Login(String defaultSchema, String username, String password, DatabaseEventCallback databaseCallback) {

			IDatabase database = Database;

			return Authenticate(database, defaultSchema, username, password, databaseCallback);
		}

		public override IQueryResponse[] ExecuteQuery(SqlQuery query) {

			// Check the interface isn't disposed (connection was closed).
			CheckNotDisposed();

			User user = User;
			IDatabaseConnection database_connection = DatabaseConnection;

			// Log this Query if Query logging is enabled
			if (Database.Context.Logger.IsInterestedIn(LogLevel.Debug)) {
				// Output the instruction to the _queries log.
				user.Database.Context.Logger.Debug(this, String.Format("[CLIENT] [{0}] [{1}] - Query: {2}", user.UserName, host_name, query.Text));
			}

			// Write debug message (Info level)
			if (Logger.IsInterestedIn(LogLevel.Debug)) {
				Logger.Debug(this, "Query From User: " + user.UserName + "@" + host_name);
				Logger.Debug(this, "Query: " + query.Text.Trim());
			}

			// Get the locking mechanism.
			LockingMechanism locker = database_connection.LockingMechanism;
			LockingMode lockMode = LockingMode.None;
			IQueryResponse[] response = null;
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
					lockMode = LockingMode.Exclusive;
					locker.SetMode(lockMode);

					// Execute the Query (behaviour for this comes from super).
					response = base.ExecuteQuery(query);

					// Return the result.
					return response;

				} finally {
					try {
						// This is executed no matter what happens.  Very important we
						// unlock the tables.
						if (lockMode != LockingMode.None) {
							locker.FinishMode(lockMode);
						}
					} catch (Exception e) {
						// If this throws an exception, we should output it to the debug
						// log and screen.
#if DEBUG
						Console.Error.WriteLine(e.Message);
						Console.Error.WriteLine(e.StackTrace);
#endif
						Logger.Error(this, "Exception finishing locks");
						Logger.Error(this, e);
						// Note, we can't throw an error here because we may already be in
						// an exception that happened in the above 'try' block.
					}
				}

			} finally {
				// This always happens after tables are unlocked.
				// Also guarenteed to happen even if something fails.

				// If we are in auto-commit mode then commit the Query here.
				// Do we auto-commit?
				if (database_connection.AutoCommit) {
					// Yes, so grab an exclusive Lock and auto-commit.
					try {
						// Lock into exclusive mode.
						locker.SetMode(LockingMode.Exclusive);
						// If an error occured then roll-back
						if (response == null) {
							// Rollback.
							database_connection.Rollback();
						} else {
							try {
								// Otherwise commit.
								database_connection.Commit();
							} catch (Exception e) {
								foreach (IQueryResponse queryResponse in response) {
									// Dispose this response if the commit failed.
									DisposeResult(queryResponse.ResultId);
								}
								// And throw the SQL Exception
								throw HandleExecuteThrowable(e, query);
							}
						}
					} finally {
						locker.FinishMode(LockingMode.Exclusive);
					}
				}

			}

		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (User != null) {
					IDatabaseConnection database = DatabaseConnection;
					LockingMechanism locker = database.LockingMechanism;
					try {
						// Lock into exclusive mode,
						locker.SetMode(LockingMode.Exclusive);
						// Roll back any open transaction.
						database.Rollback();
					} finally {
						// Finish being in exclusive mode.
						locker.FinishMode(LockingMode.Exclusive);
						// Close the database connection object.
						database.Close();
						// Log out the user
						User.Logout();
						// Call the internal dispose method.
						base.Dispose(true);
					}
				}
			}
		}
	}
}