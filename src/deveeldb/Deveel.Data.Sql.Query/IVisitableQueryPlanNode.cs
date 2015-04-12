using System;

namespace Deveel.Data.Sql.Query {
	public interface IVisitableQueryPlanNode {
		void Accept(IQueryPlanNodeVisitor visitor);
	}
}
