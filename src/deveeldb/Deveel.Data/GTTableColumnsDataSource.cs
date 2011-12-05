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
		/// The list of all DataTableInfo visible to the transaction.
		/// </summary>
		private DataTableInfo[] visible_tables;
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
			visible_tables = new DataTableInfo[list.Length];
			row_count = 0;
			for (int i = 0; i < list.Length; ++i) {
				DataTableInfo info = transaction.GetDataTableDef(list[i]);
				row_count += info.ColumnCount;
				visible_tables[i] = info;
			}
			return this;
		}

		// ---------- Implemented from GTDataSource ----------

		public override DataTableInfo DataTableInfo {
			get { return InfoDataTableInfo; }
		}

		public override int RowCount {
			get { return row_count; }
		}

		public override TObject GetCellContents(int column, int row) {

			int sz = visible_tables.Length;
			int rs = 0;
			for (int n = 0; n < sz; ++n) {
				DataTableInfo info = visible_tables[n];
				int b = rs;
				rs += info.ColumnCount;
				if (row >= b && row < rs) {
					// This is the column that was requested,
					int seq_no = row - b;
					DataTableColumnInfo colInfo = info[seq_no];
					switch (column) {
						case 0:  // schema
							return GetColumnValue(column, info.Schema);
						case 1:  // table
							return GetColumnValue(column, info.Name);
						case 2:  // column
							return GetColumnValue(column, colInfo.Name);
						case 3:  // sql_type
							return GetColumnValue(column, (BigNumber)(int)colInfo.SqlType);
						case 4:  // type_desc
							return GetColumnValue(column, colInfo.SQLTypeString);
						case 5:  // size
							return GetColumnValue(column, (BigNumber)colInfo.Size);
						case 6:  // scale
							return GetColumnValue(column, (BigNumber)colInfo.Scale);
						case 7:  // not_null
							return GetColumnValue(column, colInfo.IsNotNull);
						case 8:  // default
							return GetColumnValue(column,
											   colInfo.GetDefaultExpressionString());
						case 9:  // index_str
							return GetColumnValue(column, colInfo.IndexScheme);
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
		/// The data table info that describes this table of data source.
		/// </summary>
		internal static readonly DataTableInfo InfoDataTableInfo;

		static GTTableColumnsDataSource() {

			DataTableInfo info = new DataTableInfo();
			info.TableName = new TableName(Database.SystemSchema, "table_columns");

			// Add column definitions
			info.AddColumn(GetStringColumn("schema"));
			info.AddColumn(GetStringColumn("table"));
			info.AddColumn(GetStringColumn("column"));
			info.AddColumn(GetNumericColumn("sql_type"));
			info.AddColumn(GetStringColumn("type_desc"));
			info.AddColumn(GetNumericColumn("size"));
			info.AddColumn(GetNumericColumn("scale"));
			info.AddColumn(GetBooleanColumn("not_null"));
			info.AddColumn(GetStringColumn("default"));
			info.AddColumn(GetStringColumn("index_str"));
			info.AddColumn(GetNumericColumn("seq_no"));

			// Set to immutable
			info.SetImmutable();

			InfoDataTableInfo = info;
		}
	}
}