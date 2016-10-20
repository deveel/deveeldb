using System;

namespace Deveel.Data.Sql.Compile {
	public static class SystemBuilderExtensions {
		public static ISystemBuilder UseSqlCompiler(this ISystemBuilder builder, ISqlCompiler compiler) {
			return builder.Use<ISqlCompiler>(options => options.ToInstance(compiler).InSystemScope());
		}

		public static ISystemBuilder UseSqlCompiler<T>(this ISystemBuilder builder) where T : class, ISqlCompiler {
			return builder.Use<ISqlCompiler>(options => options.To<T>().InSystemScope());
		}

		public static ISystemBuilder UseDefaultSqlCompiler(this ISystemBuilder builder) {
			return builder.UseSqlCompiler<PlSqlCompiler>();			
		}
	}
}
