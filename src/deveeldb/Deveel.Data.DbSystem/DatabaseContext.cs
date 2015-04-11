using System;

using Deveel.Data.Caching;
using Deveel.Data.Configuration;
using Deveel.Data.Routines;
using Deveel.Data.Sql.Query;
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

				Locker.Reset();
			}

			Locker = null;
			StoreSystem = null;
		}

		public IDbConfig Configuration { get; private set; }

		public ISystemContext SystemContext { get; private set; }

		public IStoreSystem StoreSystem { get; private set; }

		public IQueryPlanner QueryPlanner { get; private set; }

		public IRoutineResolver RoutineResolver { get; private set; }

		public TableCellCache CellCache { get; private set; }

		public Locker Locker { get; private set; }

		private void Init() {
			InitStorageSystem();
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
