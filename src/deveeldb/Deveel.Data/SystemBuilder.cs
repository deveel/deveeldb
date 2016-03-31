// 
//  Copyright 2010-2016 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//


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
using Deveel.Data.Sql.Types;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Sql.Views;
using Deveel.Data.Store;

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

			ServiceContainer.Bind<IObjectManager>()
				.To<TypeManager>()
				.WithKey(DbObjectType.Type)
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
