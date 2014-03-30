// 
//  Copyright 2010-2013  Deveel
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

using Deveel.Data.Security;
using Deveel.Data.Types;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// An implementation of <see cref="IMutableTableDataSource"/> that 
	/// presents the current list of sessions on the database.
	/// </summary>
	/// <remarks>
	/// <b>Note:</b> This is not designed to be a long kept object. 
	/// It must not last beyond the lifetime of a transaction.
	/// </remarks>
	sealed class GTCurrentConnectionsDataSource : GTDataSource {
		/// <summary>
		/// The DatabaseConnection object that this is table is modelling the
		/// information within.
		/// </summary>
		private DatabaseConnection database;

		/// <summary>
		/// The list of info keys/values in this object.
		/// </summary>
		private List<CurrentConnection> connections;

		/// <summary>
		/// The data table info that describes this table of data source.
		/// </summary>
		internal static readonly DataTableInfo DataTableInfo;

		static GTCurrentConnectionsDataSource() {
			DataTableInfo info = new DataTableInfo(SystemSchema.CurrentConnections);

			// Add column definitions
			info.AddColumn("username", PrimitiveTypes.VarString);
			info.AddColumn("host_string", PrimitiveTypes.VarString);
			info.AddColumn("last_command", PrimitiveTypes.Date);
			info.AddColumn("time_connected", PrimitiveTypes.Date);

			// Set to immutable
			info.IsReadOnly = true;

			DataTableInfo = info;

		}

		public GTCurrentConnectionsDataSource(DatabaseConnection connection)
			: base(connection.Context) {
			database = connection;
			connections = new List<CurrentConnection>();
		}

		/// <summary>
		/// Initialize the data source.
		/// </summary>
		/// <returns></returns>
		public GTCurrentConnectionsDataSource Init() {
			UserManager userManager = database.Database.UserManager;

			// Synchronize over the user manager while we inspect the information,
			lock (userManager) {
				for (int i = 0; i < userManager.UserCount; ++i) {
					User user = userManager[i];
					CurrentConnection currentConnection = new CurrentConnection();
					currentConnection.UserName = user.UserName;
					currentConnection.Host = user.ConnectionString;
					currentConnection.LastCommand = user.LastCommandTime;
					currentConnection.Connected = user.TimeConnected;

					connections.Add(currentConnection);
				}
			}

			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		/// <inheritdoc/>
		public override DataTableInfo TableInfo {
			get { return DataTableInfo; }
		}

		/// <inheritdoc/>
		public override int RowCount {
			get { return connections.Count/4; }
		}

		/// <inheritdoc/>
		public override TObject GetCell(int column, int row) {
			CurrentConnection currentConnection = connections[row];

			switch (column) {
				case 0:  // username
					return GetColumnValue(column, currentConnection.UserName);
				case 1:  // host_string
					return GetColumnValue(column, currentConnection.Host);
				case 2:  // last_command
					return GetColumnValue(column, currentConnection.LastCommand);
				case 3:  // time_connected
					return GetColumnValue(column, currentConnection.Connected);
				default:
					throw new IndexOutOfRangeException();
			}
		}

		// ---------- Overwritten from GTDataSource ----------

		/// <inheritdoc/>
		protected override void Dispose(bool disposing) {
			if (disposing) {
				connections = null;
				database = null;
			}
		}

		#region CurrentConnection

		class CurrentConnection {
			public string UserName;
			public string Host;
			public DateTime LastCommand;
			public DateTime Connected;
		}

		#endregion
	}
}