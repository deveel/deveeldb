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
using System.Linq;

using Deveel.Data.Caching;
using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;
using Deveel.Data.Services;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	public sealed class DatabaseContext : IDatabaseContext/*, IResolveScope*/ {
		private ServiceContainer container;

		public DatabaseContext(ISystemContext systemContext, string name) 
			: this(systemContext, CreateSimpleConfig(name)) {
		}

		public DatabaseContext(ISystemContext systemContext, IConfiguration configuration) {
			if (systemContext == null)
				throw new ArgumentNullException("systemContext");
			if (configuration == null)
				throw new ArgumentNullException("configuration");

			container = new ServiceContainer(this, (ServiceContainer) systemContext.Container);
			container.Unregister<IConfiguration>();
			container.Register(configuration);

			SystemContext = systemContext;

			Configuration = configuration;
			Locker = new Locker(this);

			Sessions = new ActiveSessionList(this);

			InitStorageSystem();
		}

		~DatabaseContext() {
			Dispose(false);
		}

		private static IConfiguration CreateSimpleConfig(string dbName) {
			if (String.IsNullOrEmpty(dbName))
				throw new ArgumentNullException("dbName");

			var config = Data.Configuration.Configuration.Empty;
			config.SetKey(DatabaseConfigKeys.DatabaseName);
			config.SetValue(DatabaseConfigKeys.DatabaseName, dbName);
			return config;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (StoreSystem != null)
					StoreSystem.Dispose();

				if (Locker != null)
					Locker.Reset();

				if (container !=null)
					container.Dispose();
			}

			Locker = null;
			StoreSystem = null;
			container = null;
		}

		public IConfiguration Configuration { get; private set; }

		public ActiveSessionList Sessions { get; private set; }

		public ISystemContext SystemContext { get; private set; }

		public IStoreSystem StoreSystem { get; private set; }

		public Locker Locker { get; private set; }

		IServiceContainer IServiceContext.Container {
			get { return container; }
		}

		//private void Init() {
		//	InitStorageSystem();
		//}

		private void InitStorageSystem() {
			var storeSystemType = this.StorageSystemType();
			if (storeSystemType == null)
				throw new DatabaseConfigurationException("Storage system type is required.");

			try {
				var storageTypeName = Configuration.GetString(DatabaseConfigKeys.StorageSystem);
				StoreSystem = this.ResolveService<IStoreSystem>(storageTypeName);

				if (StoreSystem == null)
					throw new DatabaseConfigurationException("The storage system for the database was not set.");
			} catch(DatabaseConfigurationException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("Could not initialize the storage system", ex);
			}
		}

		private IStoreSystem CreateExternalStoreSystem(Type type) {
			return SystemContext.ResolveService(type) as IStoreSystem;
		}

		IEnumerable<KeyValuePair<string, object>> IEventSource.Metadata {
			get {
				return new Dictionary<string, object> {
					{"Database", this.DatabaseName()}
				}.AsEnumerable();
			}
		}

		IEventSource IEventSource.ParentSource {
			get { return SystemContext; }
		}

		//object IResolveScope.OnBeforeResolve(Type type, string name) {
		//	if (typeof (ITableCellCache).IsAssignableFrom(type))
		//		return cellCache;

		//	return null;
		//}

		//void IResolveScope.OnAfterResolve(Type type, string name, object obj) {
		//	if (obj is ITableCellCache)
		//		cellCache = (ITableCellCache) obj;

		//	if (obj != null && obj is IConfigurable)
		//		((IConfigurable)obj).Configure(Configuration);
		//}

		//IEnumerable IResolveScope.OnBeforeResolveAll(Type type) {
		//	return null;
		//}

		//void IResolveScope.OnAfterResolveAll(Type type, IEnumerable list) {
		//}
	}
}
