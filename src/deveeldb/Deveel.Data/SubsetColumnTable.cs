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
using System.Collections.Generic;

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
		/// The <see cref="TableInfo"/> object that describes the subset column of 
		/// this table.
		/// </summary>
		private DataTableInfo subsetTableInfo;

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

			DataTableInfo parentInfo = parent.TableInfo;
			subsetTableInfo = new DataTableInfo(parentInfo.TableName);

			for (int i = 0; i < mapping.Length; ++i) {
				int map_to = mapping[i];
				DataTableColumnInfo colInfo = parent.GetColumnInfo(map_to).Clone();
				colInfo.Name = aliases[i].Name;
				subsetTableInfo.AddColumn(colInfo);
				reverse_column_map[map_to] = i;
			}

			subsetTableInfo.IsReadOnly = true;
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
		public override DataTableInfo TableInfo {
			get { return subsetTableInfo; }
		}

		/// <inheritdoc/>
		public override VariableName GetResolvedVariable(int column) {
			return aliases[column];
		}

		/// <inheritdoc/>
		internal override SelectableScheme GetSelectableSchemeFor(int column, int originalColumn, Table table) {

			// We need to map the original_column if the original column is a reference
			// in this subset column table.  Otherwise we leave as is.
			// The reason is because FilterTable pretends the call came from its
			// parent if a request is made on this table.
			int mapped_original_column = originalColumn;
			if (table == this) {
				mapped_original_column = column_map[originalColumn];
			}

			return base.GetSelectableSchemeFor(column_map[column], mapped_original_column, table);
		}

		/// <inheritdoc/>
		internal override void SetToRowTableDomain(int column, IList<int> rowSet, ITableDataSource ancestor) {

			base.SetToRowTableDomain(column_map[column], rowSet, ancestor);
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