using System;

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Views {
	static class SystemBuilderExtensions {
		public static ISystemBuilder UseViews(this ISystemBuilder builder) {
			builder.ServiceContainer.Register<ViewsModule>();
			return builder;
		}
	}
}
