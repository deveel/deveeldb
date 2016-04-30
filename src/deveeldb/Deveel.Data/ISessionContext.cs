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

using Deveel.Data.Diagnostics;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// A context that is specific for a <see cref="ISession"/>.
	/// </summary>
	public interface ISessionContext : IEventScope, IContext {
		/// <summary>
		/// Gets a reference to the parent transaction context
		/// that originated this context.
		/// </summary>
		ITransactionContext TransactionContext { get; }

		/// <summary>
		/// Creates a context that is used by a <see cref="IQuery"/>
		/// child of the <see cref="ISession"/> that handles this context.
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="IQueryContext"/> that
		/// inherits the state from this context and is used for
		/// <see cref="IQuery"/> objects creates by the <see cref="ISession"/>
		/// that contains this context.
		/// </returns>
		IQueryContext CreateQueryContext();
	}
}

