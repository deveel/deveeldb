using Deveel.Data.Services;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Views {
	public sealed class ViewsModule : ISystemModule {
		public string ModuleName {
			get { return "System Views"; }
		}

		public string Version {
			get { return "2.0"; }
		}

		public void Register(IScope systemScope) {
			systemScope.Bind<IObjectManager>()
				.To<ViewManager>()
				.InTransactionScope()
				.WithKey(DbObjectType.View);

			systemScope.Bind<ITableCompositeCreateCallback>()
				.To<ViewsInit>()
				.WithKey("Views")
				.InTransactionScope();

			systemScope.Bind<ITableContainer>()
				.To<ViewTableContainer>()
				.InTransactionScope();
		}
	}
}