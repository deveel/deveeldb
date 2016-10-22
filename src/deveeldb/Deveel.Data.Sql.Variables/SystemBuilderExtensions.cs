using System;

using Deveel.Data.Build;
using Deveel.Data.Caching;
using Deveel.Data.Services;

namespace Deveel.Data.Sql.Variables {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseVariables(this ISystemBuilder builder) {
			return builder.Use<TableCellCache>(options => options
				.With<TableCellCache>()
				.InSystemScope());
		}
	}
}