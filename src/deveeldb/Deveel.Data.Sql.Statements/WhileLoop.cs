using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class WhileLoop : LoopBlock {
		public WhileLoop(SqlExpression conditionExpression) {
			if (conditionExpression == null)
				throw new ArgumentNullException("conditionExpression");

			ConditionExpression = conditionExpression;
		}

		public SqlExpression ConditionExpression { get; private set; }

		protected override bool Loop(ExecutionContext context) {
			// TODO: evaluate the condition against the context and return a boolean
			return base.Loop(context);
		}
	}
}
