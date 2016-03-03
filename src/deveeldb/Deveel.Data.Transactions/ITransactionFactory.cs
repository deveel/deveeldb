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

namespace Deveel.Data.Transactions {
	/// <summary>
	/// Defines the required features to factory transactions within
	/// a relational system.
	/// </summary>
	public interface ITransactionFactory {
		/// <summary>
		/// Gets the collection of currently open transactions.
		/// </summary>
		/// <value>
		/// The open transactions within the underlying system.
		/// </value>
		TransactionCollection OpenTransactions { get; }

		/// <summary>
		/// Creates a new the transaction with the isolation specified.
		/// </summary>
		/// <param name="isolation">The transaction isolation level.</param>
		/// <returns>
		/// Returns an instance of <see cref="ITransaction"/> that holds the
		/// the access to the underlying data system according to the isolation
		/// level provided. 
		/// </returns>
		ITransaction CreateTransaction(IsolationLevel isolation);
	}
}
