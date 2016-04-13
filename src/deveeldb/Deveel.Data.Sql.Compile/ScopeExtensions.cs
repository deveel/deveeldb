using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Compile {
	public static class ScopeExtensions {
		public static void UseDefaultCompiler(this IScope scope) {
			scope.Bind<ISqlCompiler>()
				.To<PlSqlCompiler>()
				.InSystemScope();
		}
	}
}
