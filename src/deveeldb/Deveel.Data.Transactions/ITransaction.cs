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

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// The simplest implementation of a transaction.
	/// </summary>
	/// <remarks>
	/// This contract allows implementors to define simple transactions
	/// that can be eventually forbid any data write operation.
	/// </remarks>
	public interface ITransaction : IVariableScope, ILockable, IDisposable {
		long CommitId { get; }

		/// <summary>
		/// Gets the isolation level of the transaction.
		/// </summary>
		TransactionIsolation Isolation { get; }

		IDatabase Database { get; }

		OldNewTableState OldNewTableState { get; }

		IObjectManagerResolver ObjectManagerResolver { get; }

		TransactionRegistry Registry { get; }

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

		void RegisterOnCommit(Action<TableCommitInfo> action);

		void UnregisterOnCommit(Action<TableCommitInfo> action);
	}
}
