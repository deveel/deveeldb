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

		public override DataTableDef TableInfo {
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