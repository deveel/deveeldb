// 
//  Copyright 2010-2011  Deveel
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

using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
	public sealed partial class DatabaseConnection {
		/// <summary>
		/// The procedure manager object for this connection.
		/// </summary>
		private RoutinesManager routinesManager;

		/// <summary>
		/// Returns the RoutinesManager object that manages database functions and
		/// procedures in the database for this connection/user.
		/// </summary>
		public RoutinesManager RoutinesManager {
			get { return routinesManager; }
		}

		/// <summary>
		/// Creates an object that implements <see cref="IProcedureConnection"/> 
		/// that provides access to this connection.
		/// </summary>
		/// <param name="user"></param>
		/// <remarks>
		/// Note that this session is set to the user of the privileges that the
		/// procedure executes under when this method returns.
		/// <para>
		/// There must be a 100% guarentee that after this method is called, a call to
		/// <see cref="DisposeProcedureConnection"/> is called which cleans up the state of this
		/// object.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		internal IProcedureConnection CreateProcedureConnection(User user) {
			// Create the IProcedureConnection object,
			DCProcedureConnection c = new DCProcedureConnection(this);
			// Record the current user
			c.previous_user = User;
			// Record the current 'close_transaction_disabled' flag
			c.transaction_disabled_flag = closeTransactionDisabled;
			// Set the new user
			User = user;
			// Disable the ability to close a transaction
			closeTransactionDisabled = true;
			// Return
			return c;
		}

		/// <summary>
		/// Disposes a <see cref="IProcedureConnection"/> previously created 
		/// by <see cref="CreateProcedureConnection"/>.
		/// </summary>
		/// <param name="connection"></param>
		internal void DisposeProcedureConnection(IProcedureConnection connection) {
			DCProcedureConnection c = (DCProcedureConnection)connection;
			// Revert back to the previous user.
			User = c.previous_user;
			// Revert back to the previous transaction disable status.
			closeTransactionDisabled = c.transaction_disabled_flag;
			// Dispose of the connection
			c.dispose();
		}

		/// <summary>
		/// An implementation of <see cref="IProcedureConnection"/> generated from 
		/// this object.
		/// </summary>
		private class DCProcedureConnection : IProcedureConnection {
			private readonly DatabaseConnection conn;
			/// <summary>
			/// The User of this connection before this procedure was started.
			/// </summary>
			internal User previous_user;

			/// <summary>
			/// The 'close_transaction_disabled' flag when this connection was created.
			/// </summary>
			internal bool transaction_disabled_flag;

			/// <summary>
			/// The ADO.NET connection created by this object.
			/// </summary>
			private IDbConnection db_connection;

			public DCProcedureConnection(DatabaseConnection conn) {
				this.conn = conn;
			}


			public IDbConnection GetDbConnection() {
				if (db_connection == null) {
					db_connection = InternalDbHelper.CreateDbConnection(conn.User, conn);
				}
				return db_connection;
			}

			public Database Database {
				get { return conn.Database; }
			}


			internal void dispose() {
				previous_user = null;
				if (db_connection != null) {
					try {
						InternalDbHelper.DisposeDbConnection(db_connection);
					} catch (Exception e) {
						conn.Logger.Error(this, "Error disposing internal connection.");
						conn.Logger.Error(this, e);
						// We don't wrap this exception
					}
				}
			}
		}
	}
}