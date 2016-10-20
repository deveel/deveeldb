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
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Sql.Query {
	static class QueryPlanNodeExtensions {
		public static IList<QueryAccessedResource> DiscoverAccessedResources(this IQueryPlanNode node) {
			return DiscoverAccessedResources(node, new Dictionary<ObjectName, QueryAccessedResource>());
		}

		public static IList<QueryAccessedResource> DiscoverAccessedResources(this IQueryPlanNode node, IDictionary<ObjectName, QueryAccessedResource> accessedResources) {
			var visitor = new QueryAccessedResourceVisitor(accessedResources);
			return visitor.Discover(node);
		}

		public static IList<QueryReference> DiscoverQueryReferences(this IQueryPlanNode node, int level) {
			return DiscoverQueryReferences(node, level, new List<QueryReference>());
		}

		public static IList<QueryReference> DiscoverQueryReferences(this IQueryPlanNode node, int level, IList<QueryReference> references) {
			// TODO:
			return references;
		}

		public static SqlBinary AsBinary(this IQueryPlanNode planNode) {
			using (var memoryStream = new MemoryStream()) {
				var serializaer = new BinarySerializer();
				serializaer.Serialize(memoryStream, planNode);
				memoryStream.Flush();
				return new SqlBinary(memoryStream.ToArray());
			}
		}
	}
}
