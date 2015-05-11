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
	/// The kind of isolation levels for a <see cref="ITransaction"/>.
	/// </summary>
	public enum TransactionIsolation {
		/// <summary>
		/// The isolation level was not specified and the system
		/// falls back to the default isolation.
		/// </summary>
		Unspecified = 0,

		/// <summary>
		/// The level that acquires locks on all the resources used by the
		/// transaction, preventing all other transactions to access them,
		/// and releases these locks at the end of the main transaction.
		/// </summary>
		Serializable,

		/// <summary>
		/// Acquires write locks on the resources used until the end of the
		/// transaction, but releases the read locks when selected data
		/// are consumed.
		/// </summary>
		ReadCommitted,

		/// <summary>
		/// The least isolated level of a transaction, when dirty selects
		/// can occur, since uncommitted data from other transactions can
		/// be seen.
		/// </summary>
		ReadUncommitted,
		RepeatableRead,
		Snapshot
	}
}