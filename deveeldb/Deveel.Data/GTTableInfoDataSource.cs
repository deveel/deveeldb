//  
//  GTTableInfoDataSource.cs
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

namespace Deveel.Data {
	/// <summary>
	/// An implementation of <see cref="IMutableTableDataSource"/> that 
	/// presents information about the tables in all schema.
	/// </summary>
	/// <remarks>
	/// <b>Note</b> This is not designed to be a long kept object. It must not 
	/// last beyond the lifetime of a transaction.
	/// </remarks>
	sealed class GTTableInfoDataSource : GTDataSource {
		/// <summary>
		/// The transaction that is the view of this information.
		/// </summary>
		private Transaction transaction;
		/// <summary>
		/// The list of all TableName visible to the transaction.
		/// </summary>
		private TableName[] table_list;
		/// <summary>
		/// The list of all table types that are visible.
		/// </summary>
		private String[] table_types;
		/// <summary>
		/// The number of rows in this table.
		/// </summary>
		private int row_count;

		public GTTableInfoDataSource(Transaction transaction)
			: base(transaction.System) {
			this.transaction = transaction;
		}

		/// <summary>
		/// Initialize the data source.
		/// </summary>
		/// <returns></returns>
		public GTTableInfoDataSource Init() {
			// All the tables
			table_list = transaction.GetTables();
			Array.Sort(table_list);
			table_types = new String[table_list.Length];
			row_count = table_list.Length;

			for (int i = 0; i < table_list.Length; ++i) {
				String cur_type = transaction.GetTableType(table_list[i]);
				// If the table is in the SYSTEM schema, the type is defined as a
				// SYSTEM TABLE.
				if (cur_type.Equals("TABLE") &&
					table_list[i].Schema.Equals("SYSTEM")) {
					cur_type = "SYSTEM TABLE";
				}
				table_types[i] = cur_type;
			}

			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		public override DataTableDef DataTableDef {
			get { return DEF_DATA_TABLE_DEF; }
		}

		public override int RowCount {
			get { return row_count; }
		}

		public override TObject GetCellContents(int column, int row) {
			TableName tname = table_list[row];
			switch (column) {
				case 0:  // schema
					return GetColumnValue(column, tname.Schema);
				case 1:  // name
					return GetColumnValue(column, tname.Name);
				case 2:  // type
					return GetColumnValue(column, table_types[row]);
				case 3:  // other
					// Table notes, etc.  (future enhancement)
					return GetColumnValue(column, "");
				default:
					throw new ApplicationException("Column out of bounds.");
			}
		}

		// ---------- Overwritten from GTDataSource ----------

		protected override void Dispose() {
			base.Dispose();
			table_list = null;
			transaction = null;
		}

		// ---------- Static ----------

		/// <summary>
		/// The data table def that describes this table of data source.
		/// </summary>
		internal static readonly DataTableDef DEF_DATA_TABLE_DEF;

		static GTTableInfoDataSource() {

			DataTableDef def = new DataTableDef();
			def.TableName = Database.SysTableInfo;

			// Add column definitions
			def.AddColumn(GetStringColumn("schema"));
			def.AddColumn(GetStringColumn("name"));
			def.AddColumn(GetStringColumn("type"));
			def.AddColumn(GetStringColumn("other"));

			// Set to immutable
			def.SetImmutable();

			DEF_DATA_TABLE_DEF = def;

		}
	}
}