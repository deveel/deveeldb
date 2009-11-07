//  
//  GTTableColumnsDataSource.cs
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

		public override DataTableDef DataTableDef {
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
							return GetColumnValue(column,
											   BigNumber.fromLong((int)col_def.SqlType));
						case 4:  // type_desc
							return GetColumnValue(column, col_def.SQLTypeString);
						case 5:  // size
							return GetColumnValue(column, BigNumber.fromLong(col_def.Size));
						case 6:  // scale
							return GetColumnValue(column, BigNumber.fromLong(col_def.Scale));
						case 7:  // not_null
							return GetColumnValue(column, col_def.IsNotNull);
						case 8:  // default
							return GetColumnValue(column,
											   col_def.GetDefaultExpressionString());
						case 9:  // index_str
							return GetColumnValue(column, col_def.IndexScheme);
						case 10:  // seq_no
							return GetColumnValue(column, BigNumber.fromLong(seq_no));
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
			def.TableName = new TableName(Database.SystemSchema, "sUSRTableColumns");

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