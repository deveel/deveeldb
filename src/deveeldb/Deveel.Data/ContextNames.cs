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

using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// A set of the default well-known names of contexts.
	/// </summary>
	/// <remarks>
	/// When a <see cref="Context"/> is created, a named scope is
	/// also created to handle the services within that context:
	/// it is possible to control in which named scope to instantiate
	/// a service by providing one of these names.
	/// </remarks>
	/// <seealso cref="IContext"/>
	/// <seealso cref="Context.ContextName"/>
	public static class ContextNames {
		/// <summary>
		/// The name of the system context.
		/// </summary>
		/// <seealso cref="ISystem"/>
		public const string System = "system";

		/// <summary>
		/// The name of the database context.
		/// </summary>
		/// <seealso cref="IDatabase"/>
		public const string Database = "database";

		/// <summary>
		/// The name of the transaction context.
		/// </summary>
		/// <seealso cref="ITransaction"/>
		public const string Transaction = "transaction";

		/// <summary>
		/// The name of the user session context.
		/// </summary>
		/// <seealso cref="ISession"/>
		public const string Session = "session";

		/// <summary>
		/// The name of the query context
		/// </summary>
		/// <seealso cref="IQuery"/>
		public const string Query = "query";

		/// <summary>
		/// The name of a single execution block context.
		/// </summary>
		/// <seealso cref="IBlock"/>
		public const string Block = "block";
	}
}
