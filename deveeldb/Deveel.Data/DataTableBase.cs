// 
//  DataTableBase.cs
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
	/// This is the abstract class implemented by a <see cref="DataTable"/> 
	/// like table.
	/// </summary>
	public abstract class DataTableBase : Table, IRootTable {
		/// <summary>
		/// Returns the fully resolved table name.
		/// </summary>
		public TableName TableName {
			get { return DataTableDef.TableName; }
		}

		/// <inheritdoc/>
		public bool TypeEquals(IRootTable table) {
			if (table is DataTableBase) {
				DataTableBase dest = (DataTableBase)table;
				return (TableName.Equals(dest.TableName));
			}
			return (this == table);
		}

		/// <inheritdoc/>
		public override String ToString() {
			return TableName + "[" + RowCount + "]";
		}
	}
}