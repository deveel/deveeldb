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
using System.Collections.Generic;

using Deveel.Data.Configurations;
using Deveel.Data.Sql;

namespace Deveel.Data.Transactions {
	public interface ITransaction : IContext, IConfigurationScope {
		/// <summary>
		/// Gets a unique identifier of the transaction
		/// </summary>
		long CommitId { get; }

		/// <summary>
		/// Gets the instance of the database that holds this
		/// transaction.
		/// </summary>
		IDatabase Database { get; }

		/// <summary>
		/// Gets the level of isolation of this transaction
		/// </summary>
		IsolationLevel IsolationLevel { get; }

		/// <summary>
		/// Makes the transaction to access the objects with the type speficied
		/// </summary>
		/// <param name="objects">The list of objects to be accessed</param>
		/// <param name="accessType">The type of access to the given objects</param>
		/// <remarks>
		/// <para>
		/// This method makes the transaction to acquire a reference to the
		/// given objects, locking them for access of the given type from
		/// other transactions, until <see cref="Exit"/> is invoked or the
		/// transaction is disposed 
		/// </para>
		/// <para>
		/// The locking of the provided resources occurrs only ove those objects
		/// that are <c>lockable</c> and according to the <see cref="IsolationLevel"/>
		/// of the transaction.
		/// </para>
		/// </remarks>
		/// <seealso cref="IsolationLevel"/>
		/// <see cref="Exit"/>
		void Enter(IEnumerable<IDbObject> objects, AccessType accessType);

		/// <summary>
		/// Releases any lock acquired by this transaction over the
		/// objects specified.
		/// </summary>
		/// <param name="objects">The list of objects to be released by this transaction</param>
		/// <param name="accessType">The type of access to be released from
		/// the given objects</param>
		void Exit(IEnumerable<IDbObject> objects, AccessType accessType);


		/// <summary>
		/// Commits all write operation done during the lifetime of 
		/// this transaction and invalidates it.
		/// </summary>
		/// <param name="savePointName">An optional name to identify the save point
		/// of this commit within the database log</param>
		/// <seealso cref="Rollback"/>
		/// <remarks>
		/// When a transaction is disposed without explicitly calling
		/// <see cref="Commit"/>, all the operations are implicitly rolled-back.
		/// </remarks>
		void Commit(string savePointName);

		/// <summary>
		/// Rollback any write operations done during the lifetime
		/// of this transaction and invalidates it.
		/// </summary>
		/// <param name="savePointName">An optional name to identify a previous
		/// save point to rollback the transaction to</param>
		/// <remarks>
		/// When a transaction is disposed without explicitly calling
		/// <see cref="Commit"/>, all the operations are implicitly rolled-back.
		/// </remarks>
		/// <seealso cref="IDisposable.Dispose"/>
		/// <seealso cref="Commit"/>
		void Rollback(string savePointName);
	}
}