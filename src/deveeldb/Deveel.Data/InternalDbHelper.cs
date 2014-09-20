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
using System.Data;

using Deveel.Data.Client;
using Deveel.Data.Configuration;
using Deveel.Data.Control;
using Deveel.Data.DbSystem;
using Deveel.Data.Protocol;
using Deveel.Data.Security;
using Deveel.Data.Sql;

namespace Deveel.Data {
	/// <summary>
	/// Helper and convenience methods and classes for creating a ADO.NET 
	/// interface that has direct access to an open transaction of a 
	/// <see cref="DatabaseConnection"/>.
	/// </summary>
	/// <remarks>
	/// This class allows us to provide ADO.NET access to stored procedures 
	/// from inside the engine.
	/// </remarks>
	static class InternalDbHelper {
		/// <summary>
		/// Returns a <see cref="IDbConnection"/> object that is bound to 
		/// the given <see cref="DatabaseConnection"/> object.
		/// </summary>
		/// <param name="user"></param>
		/// <param name="connection"></param>
		/// <remarks>
		/// Queries executed on the <see cref="IDbConnection">connection</see> 
		/// alter the currently open transaction.
		/// <para>
		/// <b>Note</b>: It is assumed that the <see cref="DatabaseConnection"/> is 
		/// locked in exclusive mode when a command is executed (eg. via the <i>ExecuteXXX</i> 
		/// methods in <see cref="Statement"/>).
		/// </para>
		/// <para>
		/// <b>Note</b>: Auto-commit is <b>DISABLED</b> for the SQL connection and 
		/// can not be enabled.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public static DeveelDbConnection CreateDbConnection(User user, IDatabaseConnection connection) {
			InternalDatabaseInterface dbInterface = new InternalDatabaseInterface(user, connection);
			return new InternalConnection(connection, dbInterface, 11, 4092000);
		}

		/// <summary>
		/// Disposes the <see cref="IDbConnection">ADO.NET connection</see> 
		/// object returned by the <see cref="CreateDbConnection"/> method.
		/// </summary>
		/// <param name="dbConnection"></param>
		/// <remarks>
		/// This should be called to free resources associated with the connection object.
		/// <para>
		/// After this has completed the given <see cref="IDbConnection">connection</see> 
		/// object in invalidated.
		/// </para>
		/// </remarks>
		public static void DisposeDbConnection(IDbConnection dbConnection) {
			InternalConnection connection = (InternalConnection)dbConnection;
			// Dispose the connection.
			connection.InternalClose();
		}



		// ---------- Inner classes ----------

		/// <summary>
		/// A derived <see cref="IDbConnection"/> class from <see cref="DeveelDbConnection"/>.
		/// </summary>
		/// <remarks>
		/// This class disables auto commit, and inherits case insensitivity 
		/// from the parent <see cref="DatabaseConnection"/>.
		/// <para>
		/// The decision to disable auto-commit was because this connection will
		/// typically be used as a sub-process for executing a complete command.
		/// Disabling auto-commit makes handling an internal connection more user
		/// friendly.  Also, toggling this flag in the DatabaseConnection in mid-
		/// command is probably a very bad idea.
		/// </para>
		/// </remarks>
		private sealed class InternalConnection : DeveelDbConnection {
			private readonly bool ignoreCase;

			public InternalConnection(IDatabaseConnection db, IDatabaseInterface dbInterface, int cacheSize, int maxSize)
				: base(String.Empty, dbInterface, cacheSize, maxSize) {
				ignoreCase = db.IsInCaseInsensitiveMode;
				// we open internal connections at construction...
				Open();

				base.AutoCommit = false;
			}

			internal override bool IsCaseInsensitiveIdentifiers {
				get { return ignoreCase; }
			}

			/// <inheritdoc/>
			/// <remarks>
			/// Auto-commit is disabled.
			/// </remarks>
			public override bool AutoCommit {
				get { return false; }
				set {
					if (value)
						throw new DataException("Auto-commit can not be enabled for an internal connection.");
				}
			}

			internal override bool InternalOpen() {
				// In this implementation the connection is already opened at 
				// the construction time...
				return true;
			}

			/// <inheritdoc/>
			/// <remarks>
			/// closing an internal connection is a no-op. An 
			/// InternalConnection should only close when the underlying
			/// transaction closes.
			/// <para>
			/// To dispose an <see cref="InternalConnection"/>, use the static 
			/// <see cref="InternalDbHelper.DisposeDbConnection"/> method.
			/// </para>
			/// </remarks>
			internal override bool InternalClose() {
				// IDEA: Perhaps we should use this as a hint to clear some caches
				//   and free up some memory.
				return true;
			}
		}

		/// <summary>
		/// An implementation of <see cref="IDatabaseInterface"/> used to execute queries 
		/// on the <see cref="DatabaseConnection"/> and return results to the ADO.NET client.
		/// </summary>
		private sealed class InternalDatabaseInterface : DatabaseInterfaceBase {
			public InternalDatabaseInterface(User user, IDatabaseConnection db)
				: base(new SingleDatabaseHandler(db.Database), db.Database.Name) {
				Init(user, db);
			}

			// ---------- Implemented from IDatabaseInterface ----------

			public override bool Login(string defaultSchema, string username, string password, DatabaseEventCallback callback) {
				// This should never be used for an internal connection.
				throw new DataException("'login' is not supported for InternalDatabaseInterface");
			}
		}

		private sealed class SingleDatabaseHandler : IDatabaseHandler {
			public SingleDatabaseHandler(IDatabase database) {
				this.database = database;
			}

			private readonly IDatabase database;

			public IDatabase GetDatabase(string name) {
				if (name != database.Name)
					throw new ArgumentException();

				return database;
			}

			public Database CreateDatabase(DbConfig config, string name, string adminUser, string adminPass) {
				throw new NotSupportedException();
			}
		}
	}
}