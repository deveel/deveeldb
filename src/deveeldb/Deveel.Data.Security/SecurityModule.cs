using System;

using Deveel.Data.Services;

namespace Deveel.Data.Security {
	class SecurityModule : ISystemModule {
		public string ModuleName {
			get { return "Security Management"; }
		}

		public string Version {
			get { return "2.0"; }
		}

		public void Register(IScope systemScope) {
			systemScope.Bind<IUserManager>()
				.To<UserManager>()
				.InQueryScope();

			systemScope.Bind<IPrivilegeManager>()
				.To<PrivilegeManager>()
				.InQueryScope();
		}
	}
}
