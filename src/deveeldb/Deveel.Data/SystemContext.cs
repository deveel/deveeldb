// 
//  Copyright 2010-2015 Deveel
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

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;
using Deveel.Data.Routines;
using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Schemas;
using Deveel.Data.Sql.Sequences;
using Deveel.Data.Sql.Triggers;
using Deveel.Data.Sql.Variables;
using Deveel.Data.Sql.Views;
using Deveel.Data.Store;
#if !PCL
using Deveel.Data.Store.Journaled;
#endif

namespace Deveel.Data {
	/// <summary>
	/// This is the context of a database system, that handles the configurations
	/// and services used by all the databases managed within this scope.
	/// </summary>
	public sealed class SystemContext : Context, ISystemContext {
		private ServiceContainer container;

		/// <summary>
		/// Initializes a new instance of the <see cref="SystemContext"/> class,
		/// using the default set of configurations
		/// </summary>
		public SystemContext()
			: this(new Configuration.Configuration()) {
		}

		/// <summary>
		/// Initializes a new instance of the <see cref="SystemContext"/> class.
		/// </summary>
		/// <param name="configuration">The configuration.</param>
		/// <exception cref="System.ArgumentNullException">If the provided <paramref name="configuration"/>
		/// is <c>null</c>.</exception>
		public SystemContext(IConfiguration configuration) {
			if (configuration == null)
				throw new ArgumentNullException("configuration");

			Configuration = configuration;
			EventRegistry = new SystemEventRegistry(this);

			container = new ServiceContainer();
			container.RegisterInstance<IConfiguration>(configuration);
			container.RegisterInstance<ISystemContext>(this);

			UseDefaults();
		}

		/// <summary>
		/// Finalizes an instance of the <see cref="SystemContext"/> class.
		/// </summary>
		~SystemContext() {
			Dispose(false);
		}

		/// <summary>
		/// Gets the system configuration object
		/// </summary>
		public IConfiguration Configuration { get; private set; }

		protected override IScope ContextScope {
			get { return container; }
		}

		protected override string ContextName {
			get { return ContextNames.System; }
		}

		/// <summary>
		/// Gets an instance of <see cref="IEventRegistry" /> that handles
		/// events happening within the context of the system.
		/// </summary>
		public IEventRegistry EventRegistry { get; private set; }

		IEnumerable<KeyValuePair<string, object>> IEventSource.Metadata {
			get { return new KeyValuePair<string, object>[0]; }
		}

		IEventSource IEventSource.ParentSource {
			get { return null; }
		}

		public DatabaseContext CreateDatabaseContext(IConfiguration configuration) {
			return new DatabaseContext(this, configuration);
		}

		IDatabaseContext ISystemContext.CreateDatabaseContext(IConfiguration configuration) {
			return CreateDatabaseContext(configuration);
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (container != null)
					container.Dispose();
			}

			container = null;
			base.Dispose(true);
		}

		private void UseDefaults() {
			this.UseDefaultSqlCompiler();
			this.UseDefaultQueryPlanner();
			this.UseDefaultTableCellCache();
			this.UseSystemFunctions();

			this.RegisterService<IObjectManager, TableManager>(DbObjectType.Table);
			this.RegisterService<IObjectManager, ViewManager>(DbObjectType.View);
			this.RegisterService<IObjectManager, SchemaManager>(DbObjectType.Schema);
			this.RegisterService<IObjectManager, TriggerManager>(DbObjectType.Trigger);
			this.RegisterService<IObjectManager, SequenceManager>(DbObjectType.Sequence);
			this.RegisterService<IObjectManager, PersistentVariableManager>(DbObjectType.Variable);

			this.RegisterService<IStoreSystem, InMemoryStorageSystem>(DefaultStorageSystemNames.Heap);
			this.RegisterService<IStoreSystem, SingleFileStoreSystem>(DefaultStorageSystemNames.SingleFile);
#if !PCL
			this.RegisterService<IStoreSystem, JournaledStoreSystem>(DefaultStorageSystemNames.Journaled);
			this.RegisterInstance(new LocalFileSystem());
#endif
		}
	}
}
