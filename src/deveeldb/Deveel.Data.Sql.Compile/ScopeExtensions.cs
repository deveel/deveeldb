using System;

using Deveel.Data.Services;
using Deveel.Data.Sql.Parser;

namespace Deveel.Data.Sql.Compile {
	public static class ScopeExtensions {
		public static void UseDefaultCompiler(this IScope scope) {
			scope.Bind<ISqlCompiler>()
				.To<SqlDefaultCompiler>()
				.InSystemScope();

			scope.Bind<ISystemDisposeCallback>()
				.To<SqlParsersDispose>();

			scope.Bind<ISystemCreateCallback>()
				.To<SqlParsersCreate>();
		}

		class SqlParsersDispose : ISystemDisposeCallback {
			public void OnDispose(ISystem system) {
				SqlParsers.PlSql.Dispose();
				SqlParsers.PlSql = null;
			}
		}

		class SqlParsersCreate : ISystemCreateCallback {
			public void OnCreated(ISystem system) {
				SqlParsers.PlSql = new SqlDefaultParser(new SqlGrammar());
			}
		}
	}
}
