// 
//  InternalTableInfo.cs
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

namespace Deveel.Data {
	/// <summary>
	/// An implementation of <see cref="IInternalTableInfo"/> that provides a 
	/// number of methods to aid in the productions of the <see cref="IInternalTableInfo"/>
	/// interface.
	/// </summary>
	/// <remarks>
	/// This leaves the <see cref="CreateInternalTable"/> method implementation 
	/// to the derived class.
	/// </remarks>
	abstract class InternalTableInfo : IInternalTableInfo {
		/// <summary>
		/// The list of table names (as TableName) that this object maintains.
		/// </summary>
		private readonly TableName[] table_list;
		/// <summary>
		/// The list of DataTableDef objects that descibe each table in the 
		/// above list.
		/// </summary>
		private readonly DataTableDef[] table_def_list;
		/// <summary>
		/// The table type of table objects returned by this method.
		/// </summary>
		private readonly String table_type;

		internal InternalTableInfo(String type, DataTableDef[] table_def_list) {
			this.table_def_list = table_def_list;
			table_type = type;
			table_list = new TableName[table_def_list.Length];
			for (int i = 0; i < table_list.Length; ++i) {
				table_list[i] = table_def_list[i].TableName;
			}
		}

		/// <inheritdoc/>
		public int TableCount {
			get { return table_list.Length; }
		}

		/// <inheritdoc/>
		public int FindTableName(TableName name) {
			for (int i = 0; i < table_list.Length; ++i) {
				if (table_list[i].Equals(name)) {
					return i;
				}
			}
			return -1;
		}

		/// <inheritdoc/>
		public TableName GetTableName(int i) {
			return table_list[i];
		}

		/// <inheritdoc/>
		public DataTableDef GetDataTableDef(int i) {
			return table_def_list[i];
		}

		/// <inheritdoc/>
		public bool ContainsTableName(TableName name) {
			return FindTableName(name) != -1;
		}

		/// <inheritdoc/>
		public String GetTableType(int i) {
			return table_type;
		}

		/// <inheritdoc/>
		public abstract IMutableTableDataSource CreateInternalTable(int index);
	}
}