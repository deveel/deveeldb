// 
//  GTConnectionInfoDataSource.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
		public override DataTableDef DataTableDef {
			get { return DEF_DATA_TABLE_DEF; }
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
		/// The data table def that describes this table of data source.
		/// </summary>
		internal static readonly DataTableDef DEF_DATA_TABLE_DEF;

		static GTConnectionInfoDataSource() {
			DataTableDef def = new DataTableDef();
			def.TableName = new TableName(Database.SystemSchema, "sUSRConnectionInfo");

			// Add column definitions
			def.AddColumn(GetStringColumn("var"));
			def.AddColumn(GetStringColumn("value"));

			// Set to immutable
			def.SetImmutable();

			DEF_DATA_TABLE_DEF = def;
		}
	}
}