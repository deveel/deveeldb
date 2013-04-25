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

using Deveel.Data.Deveel.Data;

namespace Deveel.Data {
	/// <summary>
	/// An implementation of <see cref="IMutableTableDataSource"/> that 
	/// presents the current session information.
	/// </summary>
	/// <remarks>
	/// <b>Note:</b> This is not designed to be a long kept object. 
	/// It must not last beyond the lifetime of a transaction.
	/// </remarks>
	sealed class GTConnectionInfoDataSource : GTDataSource {
		/// <summary>
		/// The DatabaseConnection object that this is table is modelling the
		/// information within.
		/// </summary>
		private DatabaseConnection database;

		/// <summary>
		/// The list of info keys/values in this object.
		/// </summary>
		private List<string> keyValuePairs;

		/// <summary>
		/// The data table info that describes this table of data source.
		/// </summary>
		public static readonly DataTableInfo DataTableInfo;

		static GTConnectionInfoDataSource() {
			DataTableInfo info = new DataTableInfo(SystemSchema.ConnectionInfo);

			// Add column definitions
			info.AddColumn("var", TType.StringType);
			info.AddColumn("value", TType.StringType);

			// Set to immutable
			info.IsReadOnly = true;

			DataTableInfo = info;
		}

		public GTConnectionInfoDataSource(DatabaseConnection connection)
			: base(connection.System) {
			database = connection;
			keyValuePairs = new List<string>();
		}

		/// <summary>
		/// Initialize the data source.
		/// </summary>
		/// <returns></returns>
		public GTConnectionInfoDataSource Init() {
			// Set up the connection info variables.
			keyValuePairs.Add("auto_commit");
			keyValuePairs.Add(database.AutoCommit ? "true" : "false");

			keyValuePairs.Add("isolation_level");
			keyValuePairs.Add(database.TransactionIsolation.ToString());

			keyValuePairs.Add("user");
			keyValuePairs.Add(database.User.UserName);

			keyValuePairs.Add("time_connection");
			keyValuePairs.Add(database.User.TimeConnected.ToString());

			keyValuePairs.Add("connection_string");
			keyValuePairs.Add(database.User.ConnectionString);

			keyValuePairs.Add("current_schema");
			keyValuePairs.Add(database.CurrentSchema);

			keyValuePairs.Add("case_insensitive_identifiers");
			keyValuePairs.Add(database.IsInCaseInsensitiveMode ? "true" : "false");

			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		/// <inheritdoc/>
		public override DataTableInfo TableInfo {
			get { return DataTableInfo; }
		}

		/// <inheritdoc/>
		public override int RowCount {
			get { return keyValuePairs.Count/2; }
		}

		/// <inheritdoc/>
		public override TObject GetCellContents(int column, int row) {
			switch (column) {
				case 0:  // var
					return GetColumnValue(column, keyValuePairs[row * 2]);
				case 1:  // value
					return GetColumnValue(column, keyValuePairs[(row * 2) + 1]);
				default:
					throw new ApplicationException("Column out of bounds.");
			}
		}

		// ---------- Overwritten from GTDataSource ----------

		/// <inheritdoc/>
		protected override void Dispose(bool disposing) {
			if (disposing) {
				keyValuePairs = null;
				database = null;
			}
		}
	}
}