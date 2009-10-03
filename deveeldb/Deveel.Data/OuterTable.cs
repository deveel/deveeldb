// 
//  OuterTable.cs
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

using Deveel.Data.Collections;

namespace Deveel.Data {
	/// <summary>
	/// A Table class for forming OUTER type results.
	/// </summary>
	/// <remarks>
	/// This takes as its constructor the base table (with no outer
	/// <b>null</b> fields) that is what the result is based on. It is 
	/// then possible to merge in tables that are ancestors.
	/// </remarks>
	class OuterTable : VirtualTable, IRootTable {
		/// <summary>
		/// The merged rows.
		/// </summary>
		public IntegerVector[] outer_rows;

		/// <summary>
		/// The row count of the outer rows.
		/// </summary>
		private int outer_row_count;

		public OuterTable(Table input_table)
			: base() {

			RawTableInformation base_table =
							  input_table.ResolveToRawTable(new RawTableInformation());
			Table[] tables = base_table.GetTables();
			IntegerVector[] rows = base_table.GetRows();

			outer_rows = new IntegerVector[rows.Length];

			// Set up the VirtualTable with this base table information,
			Init(tables);
			Set(tables, rows);

		}

		/// <summary>
		/// Merges the given table in with this table.
		/// </summary>
		/// <param name="outside_table"></param>
		public void MergeIn(Table outside_table) {
			RawTableInformation raw_table_info =
							outside_table.ResolveToRawTable(new RawTableInformation());

			// Get the base information,
			Table[] base_tables = ReferenceTables;
			IntegerVector[] base_rows = ReferenceRows;

			// The tables and rows being merged in.
			Table[] tables = raw_table_info.GetTables();
			IntegerVector[] rows = raw_table_info.GetRows();
			// The number of rows being merged in.
			outer_row_count = rows[0].Count;

			for (int i = 0; i < base_tables.Length; ++i) {
				Table btable = base_tables[i];
				int index = -1;
				for (int n = 0; n < tables.Length && index == -1; ++n) {
					if (btable == tables[n]) {
						index = n;
					}
				}

				// If the table wasn't found, then set 'NULL' to this base_table
				if (index == -1) {
					outer_rows[i] = null;
				} else {
					IntegerVector list = new IntegerVector(outer_row_count);
					outer_rows[i] = list;
					// Merge in the rows from the input table,
					IntegerVector to_merge = rows[index];
					if (to_merge.Count != outer_row_count) {
						throw new ApplicationException("Wrong size for rows being merged in.");
					}
					list.Append(to_merge);
				}

			}

		}

		// ---------- Implemented from DefaultDataTable ----------

		/// <inheritdoc/>
		public override int RowCount {
			get { return base.RowCount + outer_row_count; }
		}

		/// <inheritdoc/>
		internal override SelectableScheme GetSelectableSchemeFor(int column, int original_column,
												Table table) {

			if (column_scheme[column] == null) {
				// EFFICIENCY: We implement this with a blind search...
				SelectableScheme scheme = new BlindSearch(this, column);
				column_scheme[column] = scheme.GetSubsetScheme(this, column);
			}

			if (table == this) {
				return column_scheme[column];
			} else {
				return column_scheme[column].GetSubsetScheme(table, original_column);
			}

		}

		/// <inheritdoc/>
		public override TObject GetCellContents(int column, int row) {
			int table_num = column_table[column];
			Table parent_table = reference_list[table_num];
			if (row >= outer_row_count) {
				row = row_list[table_num][row - outer_row_count];
				return parent_table.GetCellContents(column_filter[column], row);
			} else {
				if (outer_rows[table_num] == null) {
					// Special case, handling outer entries (NULL)
					return new TObject(GetColumnDef(column).TType, null);
				} else {
					row = outer_rows[table_num][row];
					return parent_table.GetCellContents(column_filter[column], row);
				}
			}
		}

		// ---------- Implemented from IRootTable ----------

		/// <inheritdoc/>
		public bool TypeEquals(IRootTable table) {
			return (this == table);
		}
	}
}