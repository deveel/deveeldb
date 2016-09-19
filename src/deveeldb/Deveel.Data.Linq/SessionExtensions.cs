using System;

using Deveel.Data.Design;

namespace Deveel.Data.Linq {
	public static class SessionExtensions {
		public static CompiledModel GetObjectModel(this ISession session) {
			var model = session.Context.ResolveService<CompiledModel>();
			if (model == null) {
				var builder = new MapModelBuilder();

				var providers = session.Context.ResolveAllServices<IMappingContext>();
				foreach (var provider in providers) {
					provider.OnBuildMap(builder);
				}

				model = builder.CompileModel();

				session.Context.RegisterInstance(model);
			}

			return model;
		}
	}
}
