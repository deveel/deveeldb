// 
//  Copyright 2010  Deveel
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

using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Query {
	/// <summary>
	/// A node that only evaluates the child if the result can not be 
	/// found in the cache with the given unique id.
	/// </summary>
	[Serializable]
	public class CachePointNode : SingleQueryPlanNode {
		/// <summary>
		/// The unique identifier of this cache point.
		/// </summary>
		private readonly long id;

		private readonly static Object GlobLock = new Object();
		private static int GlobId;

		public CachePointNode(IQueryPlanNode child)
			: base(child) {
			lock (GlobLock) {
				id = ((int)DateTime.Now.Ticks << 16) | (GlobId & 0x0FFFF);
				++GlobId;
			}
		}

		public override Table Evaluate(IQueryContext context) {
			// Is the result available in the context?
			Table childTable = context.GetCachedNode(id);
			if (childTable == null) {
				// No so evaluate the child and cache it
				childTable = Child.Evaluate(context);
				context.PutCachedNode(id, childTable);
			}
			return childTable;
		}

		public override string Title {
			get { return "CACHE: " + id; }
		}
	}
}