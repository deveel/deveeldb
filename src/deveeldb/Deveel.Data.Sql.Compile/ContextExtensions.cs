using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Compile {
	public static class ContextExtensions {
		public static ISqlCompiler SqlCompiler(this IContext context) {
			return context.ResolveService<ISqlCompiler>();
		}
	}
}
