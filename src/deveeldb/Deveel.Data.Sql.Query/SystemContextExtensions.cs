using System;
using System.IO;
using System.Text;

namespace Deveel.Data.Sql.Query {
	public static class SystemContextExtensions {
		public static void UseDefaultQueryPlanner(this ISystemContext context) {
			context.ServiceProvider.ResolveAll<IQueryPlanner>();
			context.ServiceProvider.Register<QueryPlanner>();
			
			QueryPlanSerializers.RegisterSerializers(context);			
		}

		public static void SerializeQueryPlan(this ISystemContext context, IQueryPlanNode planNode, Stream stream) {
			using (var writer = new BinaryWriter(stream, Encoding.Unicode)) {
				context.SerializeQueryPlan(planNode, writer);
			}
		}

		public static void SerializeQueryPlan(this ISystemContext context, IQueryPlanNode node, BinaryWriter writer) {
			var nodeType = node.GetType();

			var serializers = context.ResolveServices<IQueryPlanNodeSerializer>();
			foreach (var serializer in serializers) {
				if (serializer.CanSerialize(nodeType)) {
					serializer.Serialize(node, writer);
					return;
				}
			}

			throw new InvalidOperationException(string.Format("Could not find any serializer for node type '{0}'.", nodeType));
		}

		public static IQueryPlanNode DeserializeQueryPlan(this ISystemContext context, Type nodeType, BinaryReader reader) {
			var serializers = context.ResolveServices<IQueryPlanNodeSerializer>();
			foreach (var serializer in serializers) {
				if (serializer.CanSerialize(nodeType)) {
					return (IQueryPlanNode)serializer.Deserialize(reader);
				}
			}

			throw new InvalidOperationException(string.Format("Could not find any serializer for node type '{0}'.", nodeType));
		}
	}
}
