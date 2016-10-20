using System;

using Deveel.Data.Caching;
using Deveel.Data.Services;

namespace Deveel.Data.Sql.Variables {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseVariables(this ISystemBuilder builder) {
			builder.ServiceContainer.Bind<ITableCellCache>()
				.To<TableCellCache>()
				.InSystemScope();

			return builder;
		}
	}
}