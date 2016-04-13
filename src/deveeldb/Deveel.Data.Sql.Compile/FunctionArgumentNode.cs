using System;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Compile {
	class FunctionArgumentNode {
		public string Id { get; set; }

		public SqlExpression Expression { get; set; }

		public static FunctionArgumentNode Form(PlSqlParser.ArgumentContext context) {
			if (context == null)
				return null;

			var id = Name.Simple(context.id());
			var exp = Compile.Expression.Build(context.expression_wrapper());

			return new FunctionArgumentNode {
				Id = id,
				Expression = exp
			};
		}
	}
}
