using System;

namespace Deveel.Data.Mapping {
	public static class RequestExtensions {
		public static CompiledModel CompileModel(this IRequest query) {
			var providers = query.Context.ResolveAllServices<IMappingContext>();
			var builder = new MapModelBuilder();

			foreach (var provider in providers) {
				provider.OnBuildMap(builder);
			}

			return builder.CompileModel();
		}
	}
}
