using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Compile {
	public static class SystemBuilderExtensions {
		public static ISystemBuilder UseSqlCompiler(this ISystemBuilder builder, ISqlCompiler compiler) {
			builder.ServiceContainer.Bind<ISqlCompiler>()
				.ToInstance(compiler)
				.InSystemScope();

			return builder;
		}

		public static ISystemBuilder UseSqlCompiler<T>(this ISystemBuilder builder) where T : class, ISqlCompiler {
			builder.ServiceContainer.Bind<ISqlCompiler>()
				.To<T>()
				.InSystemScope();

			return builder;
		}

		public static ISystemBuilder UseDefaultSqlCompiler(this ISystemBuilder builder) {
			return builder.UseSqlCompiler<PlSqlCompiler>();			
		}
	}
}
