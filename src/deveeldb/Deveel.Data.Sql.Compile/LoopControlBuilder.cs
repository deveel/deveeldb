using System;

using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class LoopControlBuilder {
		public static LoopControlStatement Build(PlSqlParser.ExitStatementContext context) {
			return Build(LoopControlType.Exit, context.labelName(), context.condition());
		}

		public static LoopControlStatement Build(PlSqlParser.ContinueStatementContext context) {
			return Build(LoopControlType.Continue, context.labelName(), context.condition());
		}

		private static LoopControlStatement Build(LoopControlType controlType, PlSqlParser.LabelNameContext labelContext, PlSqlParser.ConditionContext conditionContext) {
			string label = null;
			SqlExpression whenExpression = null;

			if (labelContext != null)
				label = Name.Simple(labelContext);

			if (conditionContext != null)
				whenExpression = Expression.Build(conditionContext.expression());

			return new LoopControlStatement(controlType, label, whenExpression);
		}
	}
}
