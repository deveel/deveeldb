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
	/// This is the abstract class implemented by a <see cref="DataTable"/> 
	/// like table.
	/// </summary>
	public abstract class DataTableBase : Table, IRootTable {
		/// <summary>
		/// Returns the fully resolved table name.
		/// </summary>
		public TableName TableName {
			get { return DataTableInfo.TableName; }
		}

		/// <inheritdoc/>
		bool IRootTable.TypeEquals(IRootTable table) {
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