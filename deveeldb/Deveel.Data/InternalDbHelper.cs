//  
//  InternalDbHelper.cs
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
using System.Data;

using Deveel.Data.Client;
using Deveel.Data.Server;
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
	class InternalDbHelper {
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
		/// locked in exclusive mode when a query is executed (eg. via the <i>ExecuteXXX</i> 
		/// methods in <see cref="Statement"/>).
		/// </para>
		/// <para>
		/// <b>Note</b>: Auto-commit is <b>DISABLED</b> for the SQL connection and 
		/// can not be enabled.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal static DbConnection CreateDbConnection(User user, DatabaseConnection connection) {
			InternalDatabaseInterface db_interface = new InternalDatabaseInterface(user, connection);
			return new InternalConnection(connection, db_interface, 11, 4092000);
		}

		/// <summary>
		/// Disposes the <see cref="IDbConnection">ADO.NET connection</see> 
		/// object returned by the <see cref="CreateDbConnection"/> method.
		/// </summary>
		/// <param name="db_connection"></param>
		/// <remarks>
		/// This should be called to free resources associated with the connection object.
		/// <para>
		/// After this has completed the given <see cref="IDbConnection">connection</see> 
		/// object in invalidated.
		/// </para>
		/// </remarks>
		internal static void DisposeDbConnection(IDbConnection db_connection) {
			InternalConnection connection = (InternalConnection)db_connection;
			// Dispose the connection.
			connection.InternalClose();
		}



		// ---------- Inner classes ----------

		/// <summary>
		/// A derived <see cref="IDbConnection"/> class from <see cref="DbConnection"/>.
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
		private sealed class InternalConnection : DbConnection {
			public InternalConnection(DatabaseConnection db, IDatabaseInterface db_interface, int cache_size, int max_size)
				: base(Client.ConnectionString.Empty, db_interface, cache_size, max_size) {
				IsCaseInsensitiveIdentifiers = db.IsInCaseInsensitiveMode;
				// we open internal connections at construction...
				Open();
			}

			/// <inheritdoc/>
			/// <remarks>
			/// Auto-commit is disabled.
			/// </remarks>
			public override bool AutoCommit {
				get { return false; }
				set {
					if (value) {
						throw new DataException("Auto-commit can not be enabled for an internal connection.");
					}
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
			public InternalDatabaseInterface(User user, DatabaseConnection db)
				: base(db.Database) {
				Init(user, db);
			}

			// ---------- Implemented from IDatabaseInterface ----------

			public override bool Login(String default_schema, String username, String password, IDatabaseCallBack call_back) {
				// This should never be used for an internal connection.
				throw new DataException("'login' is not supported for InterfaceDatabaseInterface");
			}

			protected override void Dispose() {
				InternalDispose();
			}
		}

	}
}