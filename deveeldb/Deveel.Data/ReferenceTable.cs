//  
//  ReferenceTable.cs
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
	/// Implementation of a Table that references a DataTable as its
	/// parent.
	/// </summary>
	/// <remarks>
	/// This is a one-to-one relationship unlike <see cref="VirtualTable"/>
	/// which is a one-to-many relationship.
	/// <para>
	/// The entire purpose of this class is as a filter. We can use it to rename
	/// a <see cref="DataTable"/> to any domain we feel like. 
	/// This allows us to generate unique column names.
	/// </para>
	/// <para>
	/// For example, say we need to join the same table. We can use this method
	/// to ensure that the newly joined table won't have duplicate column names.
	/// </para>
	/// </remarks>
	public sealed class ReferenceTable : FilterTable, IRootTable {
		/// <summary>
		/// This represents the new name of the table.
		/// </summary>
		private readonly TableName table_name;

		/// <summary>
		/// The modified DataTableDef object for this reference.
		/// </summary>
		private readonly DataTableDef modified_table_def;


		internal ReferenceTable(Table table, TableName tname)
			: base(table) {
			table_name = tname;

			// Create a modified table def based on the parent def.
			modified_table_def = new DataTableDef(table.DataTableDef);
			modified_table_def.TableName = tname;
			modified_table_def.SetImmutable();
		}

		/// <summary>
		/// Constructs the <see cref="ReferenceTable"/> given the parent 
		/// table, and a new <see cref="DataTableDef"/> that describes the 
		/// columns in this table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="def"></param>
		/// <remarks>
		/// This is used if we want to redefine the column names.
		/// <para>
		/// Note that the given DataTableDef must contain the same number of 
		/// columns as the parent table, and the columns must be the same type.
		/// </para>
		/// </remarks>
		internal ReferenceTable(Table table, DataTableDef def)
			: base(table) {
			table_name = def.TableName;

			modified_table_def = def;
		}

		/// <summary>
		/// Gets the declared name of the table.
		/// </summary>
		public TableName TableName {
			get { return table_name; }
		}

		/// <inheritdoc/>
		public override DataTableDef DataTableDef {
			get { return modified_table_def; }
		}

		/// <inheritdoc/>
		public override int FindFieldName(Variable v) {
			TableName table_name = v.TableName;
			if (table_name != null && table_name.Equals(TableName)) {
				return DataTableDef.FastFindColumnName(v.Name);
			}
			return -1;
		}

		/// <inheritdoc/>
		public override Variable GetResolvedVariable(int column) {
			return new Variable(TableName,
								DataTableDef[column].Name);
		}

		/// <inheritdoc/>
		public bool TypeEquals(IRootTable table) {
			return (this == table);
		}
	}
}