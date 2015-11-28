using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Caching;
using Deveel.Data.Configuration;
using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Schemas;
using Deveel.Data.Sql.Sequences;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Sql.Views;
using Deveel.Data.Store;
#if !PCL
using Deveel.Data.Store.Journaled;
#endif

namespace Deveel.Data {
	public class SystemBuilder {
		public SystemBuilder() 
			: this(new Configuration.Configuration()) {
		}

		public SystemBuilder(IConfiguration configuration) {
			Configuration = configuration;
			ServiceContainer = new ServiceContainer();
		}

		public IConfiguration Configuration { get; set; }

		private ServiceContainer ServiceContainer { get; set; }

		private void RegisterDefaultServices() {
			ServiceContainer.Register<SecurityModule>();

			ServiceContainer.Bind<IRoutineResolver>()
				.To<SystemFunctionsProvider>()
				.InDatabaseScope();

			ServiceContainer.Bind<ISqlCompiler>()
				.To<SqlDefaultCompiler>()
				.InSystemScope();

			ServiceContainer.Bind<IQueryPlanner>()
				.To<QueryPlanner>()
				.InSystemScope();

			ServiceContainer.Bind<ITableCellCache>()
				.To<TableCellCache>()
				.InSystemScope();

			ServiceContainer.Bind<IObjectManager>()
				.To<TableManager>()
				.WithKey(DbObjectType.Table)
				.InTransactionScope();

			ServiceContainer.Bind<IObjectManager>()
				.To<ViewManager>()
				.InTransactionScope()
				.WithKey(DbObjectType.View);

			ServiceContainer.Bind<IObjectManager>()
				.To<SequenceManager>()
				.WithKey(DbObjectType.Sequence)
				.InTransactionScope();

			ServiceContainer.Bind<IObjectManager>()
				.To<TriggerManager>()
				.WithKey(DbObjectType.Trigger)
				.InTransactionScope();

			ServiceContainer.Bind<IObjectManager>()
				.To<SchemaManager>()
				.WithKey(DbObjectType.Schema)
				.InTransactionScope();

			ServiceContainer.Bind<IObjectManager>()
				.To<PersistentVariableManager>()
				.WithKey(DbObjectType.Variable)
				.InTransactionScope();

			ServiceContainer.Bind<IObjectManager>()
				.To<RoutineManager>()
				.WithKey(DbObjectType.Routine)
				.InTransactionScope();

			ServiceContainer.Bind<IStoreSystem>()
				.To<InMemoryStorageSystem>()
				.WithKey(DefaultStorageSystemNames.Heap)
				.InDatabaseScope();

			ServiceContainer.Bind<IStoreSystem>()
				.To<SingleFileStoreSystem>()
				.WithKey(DefaultStorageSystemNames.SingleFile)
				.InDatabaseScope();

#if !PCL
			ServiceContainer.Bind<IStoreSystem>()
				.To<JournaledStoreSystem>()
				.WithKey(DefaultStorageSystemNames.Journaled)
				.InDatabaseScope();

			ServiceContainer.Bind<IFileSystem>()
				.To<LocalFileSystem>()
				.InSystemScope();
#endif
		}

		private ISystemContext BuildContext(out IEnumerable<ModuleInfo> modules) {
			RegisterDefaultServices();

			OnServiceRegistration(ServiceContainer);
			modules = LoadModules();

			return new SystemContext(Configuration, ServiceContainer);
		}

		private IEnumerable<ModuleInfo> LoadModules() {
			var moduleInfo = new List<ModuleInfo>();

			var modules = ServiceContainer.ResolveAll<ISystemModule>();
			foreach (var systemModule in modules) {
				systemModule.Register(ServiceContainer);

				moduleInfo.Add(new ModuleInfo(systemModule.ModuleName, systemModule.Version));
			}

			ServiceContainer.Unregister<ISystemModule>();
			return moduleInfo;
		}

		protected virtual void OnServiceRegistration(ServiceContainer container) {
		}

		public ISystem BuildSystem() {
			IEnumerable<ModuleInfo> modules;
			var context = BuildContext(out modules);
			return new DatabaseSystem(context, modules);
		}
	}
}
