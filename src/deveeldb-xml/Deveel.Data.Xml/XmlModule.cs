using System;

using Deveel.Data.Services;

namespace Deveel.Data.Xml {
	class XmlModule : ISystemModule {
		public void Register(IScope systemScope) {
			systemScope.Register<XmlTypeResolver>();
			systemScope.RegisterInstance(XmlFunctions.Resolver);
		}
	}
}
