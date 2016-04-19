// 
//  Copyright 2010-2016 Deveel
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

using Deveel.Data.Diagnostics;
using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	/// <summary>
	/// The simplest implementation of a transaction.
	/// </summary>
	/// <remarks>
	/// This contract allows implementors to define simple transactions
	/// that can be eventually forbid any data write operation.
	/// </remarks>
	public interface ITransaction : IDisposable {
        ITransactionContext Context { get; }

		/// <summary>
		/// Gets a number uniquely identifying a transaction within a database context.
		/// </summary>
		/// <seealso cref="TransactionCollection"/>
		int CommitId { get; }

		/// <summary>
		/// Gets the isolation level of the transaction.
		/// </summary>
		/// <seealso cref="IsolationLevel"/>
		IsolationLevel Isolation { get; }

		TransactionState State { get; }

		/// <summary>
		/// Gets the database this transaction belongs to.
		/// </summary>
		IDatabase Database { get; }

		void Enter(IEnumerable<IDbObject> objects, AccessType accessType);

		void Exit(IEnumerable<IDbObject> objects, AccessType accessType);

		/// <summary>
		/// Commits all write operation done during the lifetime of 
		/// this transaction and invalidates it.
		/// </summary>
		/// <seealso cref="TransactionExtensions.ReadOnly(ITransaction)"/>
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
		/// <seealso cref="TransactionExtensions.ReadOnly(Deveel.Data.Transactions.ITransaction)"/>
		void Rollback();
	}
}
