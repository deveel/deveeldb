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
using System.Collections.Generic;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// The simplest implementation of a transaction.
	/// </summary>
	/// <remarks>
	/// This contract allows implementors to define simple transactions
	/// that can be eventually forbit any data write operation.
	/// </remarks>
	public interface ITransaction {
		/// <summary>
		/// Gets the instance of the database the transaction is connected to.
		/// </summary>
		IDatabase Database { get; }

		/// <summary>
		/// Gets the isolation level of the transaction.
		/// </summary>
		TransactionIsolation Isolation { get; }

		/// <summary>
		/// Gets an object that manages sequences within this transaction.
		/// </summary>
		ISequenceManager SequenceManager { get; }

		/// <summary>
		/// Gets a value indicating if the transaction supports write operations.
		/// </summary>
		bool IsReadOnly { get; }

		/// <summary>
		/// Gets an instance of the <see cref="ITransactionFactory">factory</see> that generated
		/// this transaction object
		/// </summary>
		ITransactionFactory Factory { get; }

		ISystemContext SystemContext { get; }

		/// <summary>
		/// Enumerates all the objects that are accessible by the transaction.
		/// </summary>
		/// <returns>
		/// Returns an enumeration of <seealso cref="IDbObject"/> instances that can be accessed
		/// by the transaction.
		/// </returns>
		IEnumerable<IDbObject> GetObjects();

		/// <summary>
		/// 
		/// </summary>
		/// <param name="objName"></param>
		/// <returns></returns>
		bool ObjectExists(ObjectName objName);

		bool RealObjectExists(ObjectName objName);

		ObjectName TryResolveCase(ObjectName objName);

		/// <summary>
		/// Sets the current value of a table native sequence.
		/// </summary>
		/// <param name="tableName">The table name.</param>
		/// <param name="value">The current value of the native sequence.</param>
		/// <seealso cref="ISequence"/>
		/// <seealso cref="ISequenceManager"/>
		/// <returns>
		/// Returns the current table sequence value after the set.
		/// </returns>
		/// <exception cref="ObjectNotFoundException">
		/// If it was not possible to find any table having the given
		/// <paramref name="tableName">name</paramref>.
		/// </exception>
		SqlNumber SetTableId(ObjectName tableName, SqlNumber value);

		/// <summary>
		/// Gets the next value of a table native sequence.
		/// </summary>
		/// <param name="tableName"></param>
		/// <returns>
		/// Returns the next value of the sequence for the given table.
		/// </returns>
		SqlNumber NextTableId(ObjectName tableName);

		/// <summary>
		/// Finds an object for the given unique name from the underlying storage.
		/// </summary>
		/// <param name="objName">The unique name of the object to return.</param>
		/// <returns></returns>
		IDbObject GetObject(ObjectName objName);

		// Tables

		void CreateTable(TableInfo tableInfo, int dataSectorSize, int indexSectorSize);

		void CreateTemporaryTable(TableInfo tableInfo);

		void AlterTable(ObjectName tableName, TableInfo tableInfo, int dataSectorSize, int indexSectorSize);

		void DropTable(ObjectName tableName);

		void AddSelectedFromTable(ObjectName tableName);

		void CompactTable(ObjectName tableName);

		/// <summary>
		/// Commits all wirte operation done during the lifetime of 
		/// this transaction and invalidates it.
		/// </summary>
		/// <seealso cref="IsReadOnly"/>
		/// <seealso cref="Rollback"/>
		/// <remarks>
		/// When a transaction is disposed without explicitly calling
		/// <see cref="Commit"/>, all the operations are implicitly rolled-back.
		/// </remarks>
		void Commit();

		/// <summary>
		/// Rollback any write operations done during the lifetime
		/// of this transaction and invalidates it.
		/// </summary>
		/// <remarks>
		/// When a transaction is disposed without explicitly calling
		/// <see cref="Commit"/>, all the operations are implicitly rolled-back.
		/// </remarks>
		/// <seealso cref="IDisposable.Dispose"/>
		/// <seealso cref="Commit"/>
		/// <seealso cref="IsReadOnly"/>
		void Rollback();
	}
}
