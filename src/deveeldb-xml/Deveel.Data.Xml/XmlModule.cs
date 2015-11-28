using System;

using Deveel.Data.Services;

namespace Deveel.Data.Xml {
	class XmlModule : ISystemModule {
		public string ModuleName {
			get { return "Deveel.XML"; }
		}

		public string Version {
			get { return typeof (XmlModule).Assembly.GetName().Version.ToString(); }
		}

		public void Register(IScope systemScope) {
			systemScope.Register<XmlTypeResolver>();
			systemScope.RegisterInstance(XmlFunctions.Resolver);
		}
	}
}
