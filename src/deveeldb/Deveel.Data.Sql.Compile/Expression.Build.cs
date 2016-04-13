using System;

using Antlr4.Runtime.Tree;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Compile {
	static class Expression {
		public static SqlExpression Build(IParseTree tree) {
			return new SqlExpressionVisitor().Visit(tree);
		}
	}
}
