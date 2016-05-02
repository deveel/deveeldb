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

using Deveel.Data.Caching;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// A wrapper around a specialized <see cref="ICache"/> used to
	/// store and retrieve parsed <see cref="SqlStatement"/> objects.
	/// </summary>
	public sealed class StatementCache : MemoryCache, IStatementCache {
		public bool TryGet(string query, out SqlStatement[] statements) {
			if (String.IsNullOrEmpty(query)) {
				statements = null;
				return false;
			}

			object obj;
			if (!TryGetObject(query, out obj)) {
				statements = null;
				return false;
			}

			statements = (SqlStatement[]) obj;
			return true;
		}

		public void Set(string query, SqlStatement[] statements) {
			if (String.IsNullOrEmpty(query))
				return;

			SetObject(query, statements);
		}
	}
}
