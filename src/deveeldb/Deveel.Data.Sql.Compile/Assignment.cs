using System;

using Antlr4.Runtime.Misc;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Compile {
	static class Assignment {
		public static SqlStatement Statement(PlSqlParser.AssignmentStatementContext context) {
			SqlExpression varRef;

			if (context.general_element() != null) {
				var element = ElementNode.Form(context.general_element());
				if (element.Argument != null &&
					element.Argument.Length > 0)
					throw new ParseCanceledException("Invalid assignment: cannot assign a function");

				var name = element.Id;
				if (name.Parent != null)
					throw new ParseCanceledException("Invalid assignment.");

				varRef = SqlExpression.VariableReference(name.ToString());
			} else if (context.bind_variable() != null) {
				var varName = Name.Variable(context.bind_variable());
				varRef = SqlExpression.VariableReference(varName);
			} else {
				throw new ParseCanceledException("Invalid assignment syntax");
			}

			var valueExp = Expression.Build(context.expression());
			return new AssignVariableStatement(varRef, valueExp);
		}
	}
}
