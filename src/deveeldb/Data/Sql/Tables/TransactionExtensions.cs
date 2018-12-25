// 
//  Copyright 2010-2018 Deveel
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
//

using System;

using Deveel.Data.Sql.Indexes;
using Deveel.Data.Transactions;

namespace Deveel.Data.Sql.Tables {
	public static class TransactionExtensions {
		private static ObjectName ResolveReservedTableName(ObjectName tableName) {
			// We do not allow tables to be created with a reserved name
			var name = tableName.Name;

			if (tableName.Parent == null) {
				// TODO:
				//if (String.Compare(name, "OLD", StringComparison.OrdinalIgnoreCase) == 0)
				//	return SystemSchema.OldTriggerTableName;
				//if (String.Compare(name, "NEW", StringComparison.OrdinalIgnoreCase) == 0)
				//	return SystemSchema.NewTriggerTableName;
			}


			return tableName;
		}


		public static TableManager GetTableManager(this ITransaction transaction) {
			return transaction.GetObjectManager<TableManager>(DbObjectType.Table);
		}

		public static IRowIndexSet GetIndexSetForTable(this ITransaction transaction, ITableSource tableSource) {
			return transaction.GetTableManager().GetIndexSetForTable(tableSource);
		}


		/// <summary>
		/// Tries to get an object with the given name formed as table.
		/// </summary>
		/// <param name="transaction">The transaction object.</param>
		/// <param name="tableName">The name of the table to try to get.</param>
		/// <returns>
		/// Returns an instance of <see cref="ITable"/> if an object with the given name was
		/// found in the underlying transaction and it is of <see cref="DbObjectType.Table"/> and
		/// it is <c>not null</c>.
		/// </returns>
		public static ITable GetTable(this ITransaction transaction, ObjectName tableName) {
			tableName = ResolveReservedTableName(tableName);

			// TODO:
			//var tableStateHandler = transaction as ITableStateHandler;
			//if (tableStateHandler != null) {
			//	if (tableName.Equals(SystemSchema.OldTriggerTableName, transaction.IgnoreIdentifiersCase()))
			//		return tableStateHandler.TableState.OldDataTable;
			//	if (tableName.Equals(SystemSchema.NewTriggerTableName, transaction.IgnoreIdentifiersCase()))
			//		return tableStateHandler.TableState.NewDataTable;
			//}

			return transaction.GetTableManager().GetTable(tableName);
		}

		public static IMutableTable GetMutableTable(this ITransaction transaction, ObjectName tableName) {
			return transaction.GetTable(tableName) as IMutableTable;
		}
	}
}