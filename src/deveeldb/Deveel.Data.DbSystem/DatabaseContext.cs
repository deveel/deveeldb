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

using Deveel.Data.Caching;
using Deveel.Data.Configuration;
using Deveel.Data.Routines;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data.DbSystem {
	public sealed class DatabaseContext : IDatabaseContext {
		public DatabaseContext(ISystemContext systemContext, string name) 
			: this(systemContext, CreateSimpleConfig(name)) {
		}

		public DatabaseContext(ISystemContext systemContext, IDbConfig configuration) {
			if (systemContext == null)
				throw new ArgumentNullException("systemContext");
			if (configuration == null)
				throw new ArgumentNullException("configuration");

			SystemContext = systemContext;
			Configuration = configuration;
			Locker = new Locker(this);

			RoutineResolver = new SystemRoutineProvider(this);
			Sessions = new ActiveSessionList(this);

			Init();
		}

		~DatabaseContext() {
			Dispose(false);
		}

		private static IDbConfig CreateSimpleConfig(string dbName) {
			if (String.IsNullOrEmpty(dbName))
				throw new ArgumentNullException("dbName");

			var config = DbConfig.Empty;
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
			}

			Locker = null;
			StoreSystem = null;
		}

		public IDbConfig Configuration { get; private set; }

		public ActiveSessionList Sessions { get; private set; }

		public ISystemContext SystemContext { get; private set; }

		public IStoreSystem StoreSystem { get; private set; }

		public IRoutineResolver RoutineResolver { get; private set; }

		public TableCellCache CellCache { get; private set; }

		public Locker Locker { get; private set; }

		private void Init() {
			InitStorageSystem();
			InitCellCache();

			this.UseSystemFunctions();
			this.UseSystemProcedures();
		}

		private void InitCellCache() {
			var cacheMaxSize = this.CellCacheMaxSize();
			var cacheMaxCellSize = this.CellCacheMaxCellSize();
			var cacheType = this.CellCacheType();

			if (cacheType == null)
				cacheType = typeof (MemoryCache);

			ICache cache;
			if (cacheType == typeof (MemoryCache)) {
				cache = new MemoryCache(cacheMaxCellSize, cacheMaxSize, 10);
			} else {
				cache = SystemContext.ServiceProvider.Resolve(cacheType, "CellCache") as ICache;
			}

			if (cache == null)
				return;

			CellCache = new TableCellCache(this, cache, cacheMaxSize, cacheMaxCellSize);
		}

		private void InitStorageSystem() {
			var storeSystemType = this.StorageSystemType();
			if (storeSystemType == null)
				throw new DatabaseConfigurationException("Storage system type is required.");

			try {
				if (storeSystemType == typeof (InMemoryStorageSystem)) {
					StoreSystem = new InMemoryStorageSystem();
				} else {
					StoreSystem = CreateExternalStoreSystem(storeSystemType);
				}

				// TODO: File and single-file
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("Could not initialize the storage system", ex);
			}			
		}

		private IStoreSystem CreateExternalStoreSystem(Type type) {
			return SystemContext.ServiceProvider.Resolve(type) as IStoreSystem;
		}
	}
}
