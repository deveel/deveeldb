using System;

using Deveel.Data.Design;

namespace Deveel.Data.Linq {
	public static class SessionExtensions {
		public static DbCompiledModel GetObjectModel(this ISession session) {
			var compiledModel = session.Context.ResolveService<DbCompiledModel>();
			if (compiledModel == null) {
				var builder = new DbModelBuilder();

				var providers = session.Context.ResolveAllServices<IModelBuildContext>();
				foreach (var provider in providers) {
					provider.OnBuildModel(builder);
				}

				var model = builder.Build();
				compiledModel = model.Compile();

				session.Context.RegisterInstance(compiledModel);
			}

			return compiledModel;
		}
	}
}
