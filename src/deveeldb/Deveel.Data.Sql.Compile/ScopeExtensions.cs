using System;

using Deveel.Data.Services;
using Deveel.Data.Sql.Parser;

namespace Deveel.Data.Sql.Compile {
	public static class ScopeExtensions {
		public static void UseDefaultCompiler(this IScope scope) {
			scope.Bind<ISqlCompiler>()
				.To<SqlDefaultCompiler>()
				.InSystemScope();

			//scope.Bind<ISystemDisposeCallback>()
			//	.To<SqlParsersDispose>();

			//scope.Bind<ISystemCreateCallback>()
			//	.To<SqlParsersCreate>();
		}

		//class SqlParsersDispose : ISystemDisposeCallback {
		//	public void OnDispose(ISystem system) {
		//		SqlParsers.Expression.Dispose();
		//		SqlParsers.DataType.Dispose();
		//		SqlParsers.PlSql.Dispose();

		//		SqlParsers.Expression = null;
		//		SqlParsers.DataType = null;
		//		SqlParsers.PlSql = null;
		//	}
		//}

		//class SqlParsersCreate : ISystemCreateCallback {
		//	public void OnCreated(ISystem system) {
		//		SqlParsers.Expression = new SqlDefaultParser(new SqlExpressionGrammar());
		//		SqlParsers.DataType = new SqlDefaultParser(new SqlDataTypeGrammar());
		//		SqlParsers.PlSql = new SqlDefaultParser(new SqlGrammar());
		//	}
		//}
	}
}
