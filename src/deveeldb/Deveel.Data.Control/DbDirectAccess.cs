// 
//  Copyright 2010  Deveel
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

using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Threading;
using Deveel.Data.Transactions;

namespace Deveel.Data.Control {
	/// <summary>
	/// Defines a commonly accessible method to interact with the underlying
	/// database by executing commands through an authorized connection.
	/// </summary>
	public sealed class DbDirectAccess : IDisposable {
		internal DbDirectAccess(DbSystem system, User user, string defaultSchema) {
			Connect(system, user, defaultSchema);

			context = new DatabaseQueryContext(connection);
		}

		internal DbDirectAccess(DatabaseConnection connection) {
			//this constructor skips the connect process...
			this.connection = connection;

			context = new DatabaseQueryContext(connection);
		}

		private DatabaseConnection connection;
		private readonly DatabaseQueryContext context;

		/// <summary>
		/// The event raised when a trigger is fired within the current
		/// underlying connection to the database.
		/// </summary>
		public event TriggerEventHandler TriggerEvent;

		public event StatementEventHandler StatementExecuting;
		public event StatementEventHandler StatementExecuted;
		public event StatementEventHandler StatementError;

		/// <summary>
		/// Gets the user owner of the current underlying connection.
		/// </summary>
		public User User {
			get { return connection.User; }
		}

		private void Connect(DbSystem system, User user, string defaultSchema) {
			connection = system.Database.CreateNewConnection(user, TriggerNotify);

			// Put the connection in exclusive mode
			LockingMechanism locker = connection.LockingMechanism;
			locker.SetMode(LockingMode.Exclusive);
			try {
				connection.AutoCommit = true;

				// Set the default schema for this connection if it exists
				if (connection.SchemaExists(defaultSchema)) {
					connection.SetDefaultSchema(defaultSchema);
				} else {
					connection.Logger.Warning(this, "Couldn't change to '" + defaultSchema + "' schema.");
					// If we can't change to the schema then change to the APP schema
					connection.SetDefaultSchema("APP");
				}
			} finally {
				try {
					// Make sure we commit the connection.
					connection.Commit();
				} catch (TransactionException e) {
					// Just issue a warning...
					connection.Logger.Warning(this, e);
				} finally {
					// Guarentee that we unluck from EXCLUSIVE
					locker.FinishMode(LockingMode.Exclusive);
				}
			}
		}

		#region Method Implementations

		private Table ExecuteStatementImpl(Statement statement) {
			// Prepare the statement
			statement.PrepareStatement(context);

			// Evaluate the SQL statement.
			return statement.EvaluateStatement(context);
		}

		#endregion

		private void OnStatementExecuting(StatementEventArgs args) {
			if (StatementExecuting != null)
				StatementExecuting(this, args);
		}

		private void OnStatementExecuted(StatementEventArgs args) {
			if (StatementExecuted != null)
				StatementExecuted(this, args);
		}

		private void OnStatementError(StatementEventArgs args) {
			if (StatementError != null)
				StatementError(this, args);
		}

		private delegate object StatementDelegate(Statement statement);

		public IAsyncResult BeginExecuteCommand(Statement statement, AsyncCallback callback, object state) {
			StatementDelegate del = new StatementDelegate(ExecuteStatement);
			return del.BeginInvoke(statement, callback, state);
		}

		public object EndExecuteCommand(IAsyncResult result) {
			StatementDelegate del = (StatementDelegate) result.AsyncState;
			return del.EndInvoke(result);
		}

		public object ExecuteStatement(Statement statement) {
			LockingMechanism locker = connection.LockingMechanism;
			LockingMode lock_mode = LockingMode.None;
			Table response = null;

			try {
				StatementEventArgs args = new StatementEventArgs(statement);

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
					lock_mode = LockingMode.Exclusive;
					locker.SetMode(lock_mode);

					OnStatementExecuting(args);

					response = ExecuteStatementImpl(statement);

					args.SetResult(response);

					OnStatementExecuted(args);

					// Return the result.
					return response;
				} catch (Exception e) {
					StatementException error = e as StatementException;
					if (error == null)
						error = new StatementException(e.Message);

					args.SetError(error);
					OnStatementError(args);
					throw;
				} finally {
					try {
						// This is executed no matter what happens.  Very important we
						// unlock the tables.
						if (lock_mode != LockingMode.None) {
							locker.FinishMode(lock_mode);
						}
					} catch(Exception e) {
						// If this throws an exception, we should output it to the debug
						// log and screen.
						Console.Error.WriteLine(e.Message);
						Console.Error.WriteLine(e.StackTrace);
						connection.Logger.Error(this, "Exception finishing locks");
						connection.Logger.Error(this, e);
						// Note, we can't throw an error here because we may already be in
						// an exception that happened in the above 'try' block.
					}
				}

			} finally {
				// This always happens after tables are unlocked.
				// Also guarenteed to happen even if something fails.

				// If we are in auto-commit mode then commit the Query here.
				// Do we auto-commit?
				if (connection.AutoCommit) {
					// Yes, so grab an exclusive Lock and auto-commit.
					try {
						// Lock into exclusive mode.
						locker.SetMode(LockingMode.Exclusive);
						// If an error occured then roll-back
						if (response == null) {
							// Rollback.
							connection.Rollback();
						} else {
							// Otherwise commit.
							connection.Commit();
						}
					} finally {
						locker.FinishMode(LockingMode.Exclusive);
					}
				}
			}
		}


		#region Implementation of IDisposable

		public void Dispose() {
			connection.Dispose();
		}

		#endregion

		private void TriggerNotify(string triggerName, string triggerSource, TriggerEventType triggerEvent, int fireCount) {
			TriggerEventArgs args = new TriggerEventArgs(triggerName, TableName.Resolve(triggerSource), triggerEvent, fireCount);
			if (TriggerEvent != null)
				TriggerEvent(this, args);
		}
	}
}