using System;
using System.Collections.Generic;

namespace Deveel.Data.Sql.Query {
	static class QueryPlanNodeExtensions {
		public static IList<ObjectName> DiscoverTableNames(this IQueryPlanNode node, IList<ObjectName> tableNames) {
			var visitor = new QueryNodeTableNameVisitor();
			return visitor.Discover(node);
		}

		public static IList<QueryReference> DiscoverQueryReferences(this IQueryPlanNode node, int level, IList<QueryReference> references) {
			// TODO:
			return references;
		} 
	}
}
