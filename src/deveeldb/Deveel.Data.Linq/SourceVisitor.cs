using System;

using Remotion.Linq;
using Remotion.Linq.Clauses;

namespace Deveel.Data.Linq {
	class SourceVisitor : QueryModelVisitorBase {
		public SourceVisitor(ExpressionCompileContext context) {
			Context = context;
		}

		public ExpressionCompileContext Context { get; private set; }

		public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel) {
			base.VisitMainFromClause(fromClause, queryModel);
		}

		public override void VisitAdditionalFromClause(AdditionalFromClause fromClause, QueryModel queryModel, int index) {
			base.VisitAdditionalFromClause(fromClause, queryModel, index);
		}

		public override void VisitJoinClause(JoinClause joinClause, QueryModel queryModel, int index) {
			base.VisitJoinClause(joinClause, queryModel, index);
		}
	}
}
