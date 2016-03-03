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

using Deveel.Data.Configuration;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// The context of a single database within a system.
	/// </summary>
	/// <remarks>
	/// A <see cref="IDatabase"/> is wrapped around this
	/// object to obtain the configurations and systems
	/// for operations.
	/// </remarks>
	public interface IDatabaseContext : IConfigurationProvider, IContext {
		/// <summary>
		/// Gets the context of the database system that handles
		/// this database.
		/// </summary>
		/// <value>
		/// The parent system context.
		/// </value>
		ISystemContext SystemContext { get; }

		/// <summary>
		/// Gets the system that handles the storage of the data
		/// of the database.
		/// </summary>
		/// <value>
		/// The database store system.
		/// </value>
		IStoreSystem StoreSystem { get; }

        /// <summary>
        /// Creates a context to handle services and variables
        /// in the scope of a transaction.
        /// </summary>
        /// <returns></returns>
        ITransactionContext CreateTransactionContext();
	}
}
