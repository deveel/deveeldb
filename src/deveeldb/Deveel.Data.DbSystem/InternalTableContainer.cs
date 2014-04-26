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

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// An implementation of <see cref="IInternalTableContainer"/> that provides a 
	/// number of methods to aid in the productions of the <see cref="IInternalTableContainer"/>
	/// interface.
	/// </summary>
	/// <remarks>
	/// This leaves the <see cref="CreateInternalTable"/> method implementation 
	/// to the derived class.
	/// </remarks>
	abstract class InternalTableContainer : IInternalTableContainer {
		/// <summary>
		/// The list of table names (as TableName) that this object maintains.
		/// </summary>
		private readonly TableName[] tableList;
		/// <summary>
		/// The list of DataTableInfo objects that descibe each table in the 
		/// above list.
		/// </summary>
		private readonly DataTableInfo[] tableInfoList;
		/// <summary>
		/// The table type of table objects returned by this method.
		/// </summary>
		private readonly string tableType;

		internal InternalTableContainer(string type, DataTableInfo[] tableInfoList) {
			this.tableInfoList = tableInfoList;
			tableType = type;
			tableList = new TableName[tableInfoList.Length];
			for (int i = 0; i < tableList.Length; ++i) {
				tableList[i] = tableInfoList[i].TableName;
			}
		}

		/// <inheritdoc/>
		public int TableCount {
			get { return tableList.Length; }
		}

		/// <inheritdoc/>
		public int FindTableName(TableName name) {
			for (int i = 0; i < tableList.Length; ++i) {
				if (tableList[i].Equals(name)) {
					return i;
				}
			}
			return -1;
		}

		/// <inheritdoc/>
		public TableName GetTableName(int i) {
			return tableList[i];
		}

		/// <inheritdoc/>
		public DataTableInfo GetTableInfo(int i) {
			return tableInfoList[i];
		}

		/// <inheritdoc/>
		public bool ContainsTable(TableName name) {
			return FindTableName(name) != -1;
		}

		/// <inheritdoc/>
		public String GetTableType(int i) {
			return tableType;
		}

		/// <inheritdoc/>
		public abstract ITableDataSource CreateInternalTable(int index);
	}
}