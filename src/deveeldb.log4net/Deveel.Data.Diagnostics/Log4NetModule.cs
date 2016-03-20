using System;

using Deveel.Data.Services;

namespace Deveel.Data.Diagnostics {
	public sealed class Log4NetModule : ISystemModule {
		public string ModuleName {
			get { return "DeveelDB.Log4Net"; }
		}

		public string Version {
			get { return typeof (Log4NetModule).Assembly.GetName().Version.ToString(); }
		}

		public void Register(IScope systemScope) {
			Log4NetEventRouter.Setup();

			systemScope.Bind<IEventRouter>()
				.To<Log4NetEventRouter>()
				.InSystemScope();
		}
	}
}
