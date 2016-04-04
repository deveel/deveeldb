using System;

using Deveel.Data.Services;

namespace Deveel.Data.Store {
	public static class ScopeExtensions {
		public static void UseLocalFileSystem(this IScope scope) {
#if PCL
			scope.Bind<IFileSystem>()
				.To<PortableFileSystem>()
				.InSystemScope()
				.WithKey("local");
#else
			scope.Bind<IFileSystem>()
				.To<LocalFileSystem>()
				.WithKey("local")
				.InSystemScope();

#endif
		}
	}
}