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
using System.IO;
using System.Text;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Query {
	public static class SystemContextExtensions {
		public static void SerializeQueryPlan(this ISystemContext context, IQueryPlanNode planNode, Stream stream) {
			using (var writer = new BinaryWriter(stream, Encoding.Unicode)) {
				context.SerializeQueryPlan(planNode, writer);
			}
		}

		public static void SerializeQueryPlan(this ISystemContext context, IQueryPlanNode node, BinaryWriter writer) {
			var nodeType = node.GetType();

			var serializers = context.ResolveAllServices<IQueryPlanNodeSerializer>();
			foreach (var serializer in serializers) {
				if (serializer.CanSerialize(nodeType)) {
					serializer.Serialize(node, writer);
					return;
				}
			}

			throw new InvalidOperationException(string.Format("Could not find any serializer for node type '{0}'.", nodeType));
		}

		public static IQueryPlanNode DeserializeQueryPlan(this ISystemContext context, Type nodeType, BinaryReader reader) {
			var serializers = context.ResolveAllServices<IQueryPlanNodeSerializer>();
			foreach (var serializer in serializers) {
				if (serializer.CanSerialize(nodeType)) {
					return (IQueryPlanNode)serializer.Deserialize(reader);
				}
			}

			throw new InvalidOperationException(string.Format("Could not find any serializer for node type '{0}'.", nodeType));
		}
	}
}
