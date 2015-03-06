// 
//  Copyright 2010-2015 Deveel
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
using System.IO;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// Provides some convenience extension methods to <see cref="ITransaction"/> instances.
	/// </summary>
	public static class TransactionExtensions {
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
			var obj = transaction.GetObject(tableName);
			if (obj == null || obj.ObjectType != DbObjectType.Table)
				return null;

			return obj as ITable;
		}

		public static IMutableTable GetMutableTable(this ITransaction transaction, ObjectName tableName) {
			return transaction.GetTable(tableName) as IMutableTable;
		}

		public static SqlNumber NextValue(this ITransaction transaction, ObjectName sequenceName) {
			return transaction.SequenceManager.NextValue(sequenceName);
		}

		public static SqlNumber LastValue(this ITransaction transaction, ObjectName sequenceName) {
			return transaction.SequenceManager.LastValue(sequenceName);
		}

		public static SqlNumber SetValue(this ITransaction transaction, ObjectName sequenceName, SqlNumber value) {
			return transaction.SequenceManager.SetValue(sequenceName, value);
		}

		/// <summary>
		/// Creates a new table within this transaction with the given sector 
		/// size.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableInfo"></param>
		/// <remarks>
		/// This should only be called under an exclusive lock on the connection.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table already exists.
		/// </exception>
		public static void CreateTable(this ITransaction transaction, TableInfo tableInfo) {
			// data sector size defaults to 251
			// index sector size defaults to 1024
			transaction.CreateTable(tableInfo, 251, 1024);
		}

		/// <summary>
		/// Alters the table with the given name within this transaction to the
		/// specified table definition.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableName"></param>
		/// <param name="tableInfo"></param>
		/// <remarks>
		/// This should only be called under an exclusive lock on the connection.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table does not exist.
		/// </exception>
		public static void AlterTable(this ITransaction transaction, ObjectName tableName, TableInfo tableInfo) {
			// Make sure we remember the current sector size of the altered table so
			// we can create the new table with the original size.
			try {
				// HACK: We use index sector size of 2043 for all altered tables
				transaction.AlterTable(tableName, tableInfo, -1, 2043);
			} catch (IOException e) {
				throw new Exception("IO Error: " + e.Message);
			}
		}

		/// <summary>
		/// Given a DataTableInfo, if the table exists then it is updated otherwise
		/// if it doesn't exist then it is created.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableInfo"></param>
		/// <param name="dataSectorSize"></param>
		/// <param name="indexSectorSize"></param>
		/// <remarks>
		/// This should only be used as very fine grain optimization for 
		/// creating/altering tables. If in the future the underlying table 
		/// model is changed so that the given <paramref name="dataSectorSize"/>
		/// and <paramref name="indexSectorSize"/> values are unapplicable, 
		/// then the value will be ignored.
		/// </remarks>
		public static void AlterCreateTable(this ITransaction transaction, TableInfo tableInfo, int dataSectorSize, int indexSectorSize) {
			if (!transaction.ObjectExists(tableInfo.TableName)) {
				transaction.CreateTable(tableInfo, dataSectorSize, indexSectorSize);
			} else {
				transaction.AlterTable(tableInfo.TableName, tableInfo, dataSectorSize, indexSectorSize);
			}
		}
	}
}
