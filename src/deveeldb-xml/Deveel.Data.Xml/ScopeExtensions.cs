using System;

using Deveel.Data.Services;

namespace Deveel.Data.Xml {
	public static class ScopeExtensions {
		public static void UseXml(this IScope scope) {
			scope.Register<ISystemModule, XmlModule>();
		}
	}
}
