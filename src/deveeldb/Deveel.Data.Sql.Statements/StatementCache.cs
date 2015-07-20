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

using Deveel.Data.Caching;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// A wrapper around a specialized <see cref="ICache"/> used to
	/// store and retrieve parsed <see cref="SqlStatement"/> objects.
	/// </summary>
	public sealed class StatementCache {
		/// <summary>
		/// Constructs the object around the provided cache handler.
		/// </summary>
		/// <param name="cache">The <see cref="ICache"/> instance used to store the
		/// compiled statements.</param>
		public StatementCache(ICache cache) {
			Cache = cache;
		}

		public ICache Cache { get; private set; }

		public bool TryGet(string query, out IEnumerable<SqlStatement> statements) {
			throw new NotImplementedException();
		}

		public void Set(string query, IEnumerable<SqlStatement> statements) {
			throw new NotImplementedException();
		}
	}
}
