using System;

using Deveel.Data.Build;

namespace Deveel.Data.Sql.Compile {
	public static class SystemBuilderExtensions {
		public static ISystemBuilder UseSqlCompiler(this ISystemBuilder builder, ISqlCompiler compiler) {
			return builder.Use<ISqlCompiler>(options => options.With(compiler).InSystemScope());
		}

		public static ISystemBuilder UseSqlCompiler<T>(this ISystemBuilder builder) where T : class, ISqlCompiler {
			return builder.Use<ISqlCompiler>(options => options.With<T>().InSystemScope());
		}

		public static ISystemBuilder UseDefaultSqlCompiler(this ISystemBuilder builder) {
			return builder.UseSqlCompiler<PlSqlCompiler>();			
		}
	}
}
