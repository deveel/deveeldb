//  
//  SubsetColumnTable.cs
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

using Deveel.Data.Collections;

namespace Deveel.Data {
	/// <summary>
	/// Filter table placed at the top of a <see cref="Table"/>.
	/// </summary>
	/// <remarks>
	/// <para>
	/// The purpose is to only provide a view of the columns that are required.
	/// </para>
	/// In a Select query it may create a query with only the subset of columns 
	/// that were originally in the table set. This object allows to provide an
	/// interface to only the columns that the table is allowed to access.
	/// <para>
	/// The class implements <see cref="IRootTable"/> which 
	/// means a union operation will not decend further past this table when 
	/// searching for the roots.
	/// </para>
	/// </remarks>
	public sealed class SubsetColumnTable : FilterTable, IRootTable {
		/// <summary>
		/// Maps from the column in this table to the column in the parent table.
		/// </summary>
		/// <remarks>
		/// The number of entries of this should match the number of columns in this 
		/// table.
		/// </remarks>
		private int[] column_map;

		/// <summary>
		/// Maps from the column in the parent table, to the column in this table.
		/// </summary>
		/// <remarks>
		/// The size of this should match the number of columns in the parent table.
		/// </remarks>
		private int[] reverse_column_map;

		/// <summary>
		/// The <see cref="DataTableDef"/> object that describes the subset column of 
		/// this table.
		/// </summary>
		private DataTableDef subset_table_def;

		/// <summary>
		/// The resolved <see cref="VariableName"/> aliases for this subset.
		/// </summary>
		/// <remarks>
		/// These are returned by <see cref="GetResolvedVariable"/> and used in searches for 
		/// <see cref="FindFieldName"/>. This can be used to remap the variable names 
		/// used to match the columns.
		/// </remarks>
		private VariableName[] aliases;


		///<summary>
		///</summary>
		///<param name="parent"></param>
		public SubsetColumnTable(Table parent)
			: base(parent) {
		}

		/// <summary>
		/// Adds a column map into this table.
		/// </summary>
		/// <param name="mapping">The array containing a map to the column in 
		/// the parent table that we want the column number to reference.</param>
		/// <param name="aliases"></param>
		public void SetColumnMap(int[] mapping, VariableName[] aliases) {
			reverse_column_map = new int[parent.ColumnCount];
			for (int i = 0; i < reverse_column_map.Length; ++i) {
				reverse_column_map[i] = -1;
			}
			column_map = mapping;

			this.aliases = aliases;

			subset_table_def = new DataTableDef();
			DataTableDef parent_def = parent.DataTableDef;
			subset_table_def.TableName = parent_def.TableName;

			for (int i = 0; i < mapping.Length; ++i) {
				int map_to = mapping[i];
				DataTableColumnDef col_def =
								  new DataTableColumnDef(parent.GetColumnDef(map_to));
				col_def.Name = aliases[i].Name;
				subset_table_def.AddVirtualColumn(col_def);
				reverse_column_map[map_to] = i;
			}

			subset_table_def.SetImmutable();
		}

		/// <inheritdoc/>
		public override int ColumnCount {
			get { return aliases.Length; }
		}

		/// <inheritdoc/>
		public override int FindFieldName(VariableName v) {
			for (int i = 0; i < aliases.Length; ++i) {
				if (v.Equals(aliases[i])) {
					return i;
				}
			}
			return -1;
		}

		/// <inheritdoc/>
		public override DataTableDef DataTableDef {
			get { return subset_table_def; }
		}

		/// <inheritdoc/>
		public override VariableName GetResolvedVariable(int column) {
			return aliases[column];
		}

		/// <inheritdoc/>
		internal override SelectableScheme GetSelectableSchemeFor(int column, int original_column, Table table) {

			// We need to map the original_column if the original column is a reference
			// in this subset column table.  Otherwise we leave as is.
			// The reason is because FilterTable pretends the call came from its
			// parent if a request is made on this table.
			int mapped_original_column = original_column;
			if (table == this) {
				mapped_original_column = column_map[original_column];
			}

			return base.GetSelectableSchemeFor(column_map[column],
												mapped_original_column, table);
		}

		/// <inheritdoc/>
		internal override void SetToRowTableDomain(int column, IntegerVector row_set,
									   ITableDataSource ancestor) {

			base.SetToRowTableDomain(column_map[column], row_set, ancestor);
		}

		/// <inheritdoc/>
		internal override RawTableInformation ResolveToRawTable(RawTableInformation info) {
			throw new ApplicationException("Tricky to implement this method!");
			// ( for a SubsetColumnTable that is )
		}

		/// <inheritdoc/>
		public override TObject GetCellContents(int column, int row) {
			return parent.GetCellContents(column_map[column], row);
		}

		// ---------- Implemented from IRootTable ----------

		/// <inheritdoc/>
		public bool TypeEquals(IRootTable table) {
			return (this == table);
		}


		/// <inheritdoc/>
		public override String ToString() {
			String name = "SCT" + GetHashCode();
			return name + "[" + RowCount + "]";
		}
	}
}