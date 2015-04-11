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

using Deveel.Data.Caching;
using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.DbSystem {
	//TODO: Add many more functions ... this is a sort of placeholder for the moment
	/// <summary>
	/// Provides a context for executing queries, accessing the
	/// system resources and evaluation context.
	/// </summary>
	public interface IQueryContext : IDisposable {
		IUserSession Session { get; }

		IDatabaseContext DatabaseContext { get; }

		string CurrentSchema { get; }

		ICache TableCache { get; }

		IQueryContext ParentContext { get; }

		/// <summary>
		/// Computes a new random number, that is ensured to be unique 
		/// within the execution context.
		/// </summary>
		/// <param name="bitSize">The number of bits the final random number must 
		/// have. This number can only be 2, 4, 8 or 16.</param>
		/// <returns>
		/// Returns a <see cref="SqlNumber"/> that represents a unique random number
		/// computed within this execution context.
		/// </returns>
		SqlNumber NextRandom(int bitSize);
	}
}