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

using Deveel.Math;

namespace Deveel.Data {
	/// <summary>
	/// An implementation of <see cref="IMutableTableDataSource"/> that 
	/// presents information about the columns of all tables in all schema.
	/// </summary>
	/// <remarks>
	/// <b>Note</b> This is not designed to be a long kept object. It must not last
	/// beyond the lifetime of a transaction.
	/// </remarks>
	sealed class GTTableColumnsDataSource : GTDataSource {
		/// <summary>
		/// The transaction that is the view of this information.
		/// </summary>
		private Transaction transaction;
		/// <summary>
		/// The list of all DataTableDef visible to the transaction.
		/// </summary>
		private DataTableDef[] visible_tables;
		/// <summary>
		/// The number of rows in this table.
		/// </summary>
		private int row_count;

		public GTTableColumnsDataSource(Transaction transaction)
			: base(transaction.System) {
			this.transaction = transaction;
		}

		/// <summary>
		/// Initialize the data source.
		/// </summary>
		/// <returns></returns>
		public GTTableColumnsDataSource Init() {
			// All the tables
			TableName[] list = transaction.GetTables();
			visible_tables = new DataTableDef[list.Length];
			row_count = 0;
			for (int i = 0; i < list.Length; ++i) {
				DataTableDef def = transaction.GetDataTableDef(list[i]);
				row_count += def.ColumnCount;
				visible_tables[i] = def;
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

			int sz = visible_tables.Length;
			int rs = 0;
			for (int n = 0; n < sz; ++n) {
				DataTableDef def = visible_tables[n];
				int b = rs;
				rs += def.ColumnCount;
				if (row >= b && row < rs) {
					// This is the column that was requested,
					int seq_no = row - b;
					DataTableColumnDef col_def = def[seq_no];
					switch (column) {
						case 0:  // schema
							return GetColumnValue(column, def.Schema);
						case 1:  // table
							return GetColumnValue(column, def.Name);
						case 2:  // column
							return GetColumnValue(column, col_def.Name);
						case 3:  // sql_type
							return GetColumnValue(column, (BigNumber)(int)col_def.SqlType);
						case 4:  // type_desc
							return GetColumnValue(column, col_def.SQLTypeString);
						case 5:  // size
							return GetColumnValue(column, (BigNumber)col_def.Size);
						case 6:  // scale
							return GetColumnValue(column, (BigNumber)col_def.Scale);
						case 7:  // not_null
							return GetColumnValue(column, col_def.IsNotNull);
						case 8:  // default
							return GetColumnValue(column,
											   col_def.GetDefaultExpressionString());
						case 9:  // index_str
							return GetColumnValue(column, col_def.IndexScheme);
						case 10:  // seq_no
							return GetColumnValue(column, (BigNumber)seq_no);
						default:
							throw new ApplicationException("Column out of bounds.");
					}
				}

			}  // for each visible table

			throw new ApplicationException("Row out of bounds.");
		}

		// ---------- Overwritten ----------

		protected override void Dispose() {
			base.Dispose();
			visible_tables = null;
			transaction = null;
		}

		// ---------- Static ----------

		/// <summary>
		/// The data table def that describes this table of data source.
		/// </summary>
		internal static readonly DataTableDef DEF_DATA_TABLE_DEF;

		static GTTableColumnsDataSource() {

			DataTableDef def = new DataTableDef();
			def.TableName = new TableName(Database.SystemSchema, "table_columns");

			// Add column definitions
			def.AddColumn(GetStringColumn("schema"));
			def.AddColumn(GetStringColumn("table"));
			def.AddColumn(GetStringColumn("column"));
			def.AddColumn(GetNumericColumn("sql_type"));
			def.AddColumn(GetStringColumn("type_desc"));
			def.AddColumn(GetNumericColumn("size"));
			def.AddColumn(GetNumericColumn("scale"));
			def.AddColumn(GetBooleanColumn("not_null"));
			def.AddColumn(GetStringColumn("default"));
			def.AddColumn(GetStringColumn("index_str"));
			def.AddColumn(GetNumericColumn("seq_no"));

			// Set to immutable
			def.SetImmutable();

			DEF_DATA_TABLE_DEF = def;
		}
	}
}