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
using System.Collections.Generic;
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
			var connString = new DeveelDbConnectionStringBuilder {
				Host = "Heap", 
				UserName = user.UserName,
				IgnoreIdentifiersCase = connection.IsInCaseInsensitiveMode,
				Database = connection.Database.Name,
				Schema = connection.CurrentSchema,
				RowCacheSize = 1024,
				MaxCacheSize = 2048 * 1000
			};

			var connector = new InternalClientConnector(user, connection);
			var dbConn = new DeveelDbConnection(connString.ToString(), connector);
			dbConn.Open();
			return dbConn;
		}

		class SessionDatabaseHandler : IDatabaseHandler {
			private readonly IDatabaseConnection connection;

			public SessionDatabaseHandler(IDatabaseConnection connection) {
				this.connection = connection;
			}

			public IDatabase GetDatabase(string name) {
				if (!String.IsNullOrEmpty(name) && 
					!String.Equals(name, connection.Database.Name))
					return null;

				return connection.Database;
			}
		}

		#region InternalServerConnector

		class InternalServerConnector : EmbeddedServerConnector {
			private readonly User user;
			private readonly IDatabaseConnection connection;

			public InternalServerConnector(User user, IDatabaseConnection connection) 
				: base(new SessionDatabaseHandler(connection)) {
				this.user = user;
				this.connection = connection;
			}

			protected override IQueryResponse[] ExecuteQuery(string text, IEnumerable<SqlQueryParameter> parameters) {
				return CoreExecuteQuery(text, parameters);
			}

			protected override bool Authenticate(string defaultSchema, string username, string password) {
				if (user.UserName != username)
					return false;

				Session = new AuthenticatedSession(user, connection);
				return true;
			}
		}

		#endregion

		#region InternalClientConnector

		class InternalClientConnector : EmbeddedClientConnector {
			internal InternalClientConnector(User user, IDatabaseConnection connection) 
				: base(new InternalServerConnector(user, connection)) {
			}
		}

		#endregion
	}
}