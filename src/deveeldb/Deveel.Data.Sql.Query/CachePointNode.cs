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

using Deveel.Data;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	sealed class CachePointNode : SingleQueryPlanNode {
		private readonly static Object GlobLock = new Object();
		private static int GlobId;

		public CachePointNode(IQueryPlanNode child)
			: this(child,NewId()) {
		}

		public CachePointNode(IQueryPlanNode child, long id)
			: base(child) {
			Id = id;
		}

		private static long NewId() {
			long id;
			lock (GlobLock) {
				id = ((int)DateTime.Now.Ticks << 16) | (GlobId & 0x0FFFF);
				++GlobId;
			}
			return id;
		}

		public long Id { get; private set; }

		public override ITable Evaluate(IQuery context) {
			// Is the result available in the context?
			var childTable = context.GetCachedTable(Id.ToString());
			if (childTable == null) {
				// No so evaluate the child and cache it
				childTable = Child.Evaluate(context);
				context.CacheTable(Id.ToString(), childTable);
			}

			return childTable;
		}
	}
}