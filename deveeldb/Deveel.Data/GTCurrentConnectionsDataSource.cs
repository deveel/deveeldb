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
		private ArrayList key_value_pairs;

		public GTCurrentConnectionsDataSource(DatabaseConnection connection)
			: base(connection.System) {
			database = connection;
			key_value_pairs = new ArrayList();
		}

		/// <summary>
		/// Initialize the data source.
		/// </summary>
		/// <returns></returns>
		public GTCurrentConnectionsDataSource Init() {

			UserManager user_manager = database.Database.UserManager;
			// Synchronize over the user manager while we inspect the information,
			lock (user_manager) {
				for (int i = 0; i < user_manager.UserCount; ++i) {
					User user = user_manager[i];
					key_value_pairs.Add(user.UserName);
					key_value_pairs.Add(user.ConnectionString);
					key_value_pairs.Add(user.LastCommandTime);
					key_value_pairs.Add(user.TimeConnected);
				}
			}

			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		/// <inheritdoc/>
		public override DataTableDef DataTableDef {
			get { return DEF_DATA_TABLE_DEF; }
		}

		/// <inheritdoc/>
		public override int RowCount {
			get { return key_value_pairs.Count/4; }
		}

		/// <inheritdoc/>
		public override TObject GetCellContents(int column, int row) {
			switch (column) {
				case 0:  // username
					return GetColumnValue(column, key_value_pairs[row * 4]);
				case 1:  // host_string
					return GetColumnValue(column, key_value_pairs[(row * 4) + 1]);
				case 2:  // last_command
					return GetColumnValue(column, (DateTime)key_value_pairs[(row * 4) + 2]);
				case 3:  // time_connected
					return GetColumnValue(column, (DateTime)key_value_pairs[(row * 4) + 3]);
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
		/// The data table def that describes this table of data source.
		/// </summary>
		internal static readonly DataTableDef DEF_DATA_TABLE_DEF;

		static GTCurrentConnectionsDataSource() {

			DataTableDef def = new DataTableDef();
			def.TableName = new TableName(Database.SystemSchema, "sUSRCurrentConnections");

			// Add column definitions
			def.AddColumn(GetStringColumn("username"));
			def.AddColumn(GetStringColumn("host_string"));
			def.AddColumn(GetDateColumn("last_command"));
			def.AddColumn(GetDateColumn("time_connected"));

			// Set to immutable
			def.SetImmutable();

			DEF_DATA_TABLE_DEF = def;

		}
	}
}