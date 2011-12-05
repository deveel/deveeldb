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
using System.Collections;

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
		private ArrayList key_value_pairs;

		public GTConnectionInfoDataSource(DatabaseConnection connection)
			: base(connection.System) {
			database = connection;
			key_value_pairs = new ArrayList();
		}

		/// <summary>
		/// Initialize the data source.
		/// </summary>
		/// <returns></returns>
		public GTConnectionInfoDataSource Init() {

			// Set up the connection info variables.
			key_value_pairs.Add("auto_commit");
			key_value_pairs.Add(database.AutoCommit ? "true" : "false");

			key_value_pairs.Add("isolation_level");
			key_value_pairs.Add(database.TransactionIsolation.ToString());

			key_value_pairs.Add("user");
			key_value_pairs.Add(database.User.UserName);

			key_value_pairs.Add("time_connection");
			key_value_pairs.Add(database.User.TimeConnected.ToString());

			key_value_pairs.Add("connection_string");
			key_value_pairs.Add(database.User.ConnectionString);

			key_value_pairs.Add("current_schema");
			key_value_pairs.Add(database.CurrentSchema);

			key_value_pairs.Add("case_insensitive_identifiers");
			key_value_pairs.Add(database.IsInCaseInsensitiveMode ? "true" : "false");

			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		/// <inheritdoc/>
		public override DataTableInfo DataTableInfo {
			get { return InfoDataTableInfo; }
		}

		/// <inheritdoc/>
		public override int RowCount {
			get { return key_value_pairs.Count/2; }
		}

		/// <inheritdoc/>
		public override TObject GetCellContents(int column, int row) {
			switch (column) {
				case 0:  // var
					return GetColumnValue(column, key_value_pairs[row * 2]);
				case 1:  // value
					return GetColumnValue(column, key_value_pairs[(row * 2) + 1]);
				default:
					throw new ApplicationException("Column out of bounds.");
			}
		}

		// ---------- Overwritten from GTDataSource ----------

		/// <inheritdoc/>
		protected override void Dispose() {
			base.Dispose();
			key_value_pairs = null;
			database = null;
		}

		// ---------- Static ----------

		/// <summary>
		/// The data table info that describes this table of data source.
		/// </summary>
		internal static readonly DataTableInfo InfoDataTableInfo;

		static GTConnectionInfoDataSource() {
			DataTableInfo info = new DataTableInfo();
			info.TableName = new TableName(Database.SystemSchema, "sUSRConnectionInfo");

			// Add column definitions
			info.AddColumn(GetStringColumn("var"));
			info.AddColumn(GetStringColumn("value"));

			// Set to immutable
			info.SetImmutable();

			InfoDataTableInfo = info;
		}
	}
}