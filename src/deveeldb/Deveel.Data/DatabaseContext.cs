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
	public sealed class DatabaseContext : Context, IDatabaseContext/*, IResolveScope*/ {
		public DatabaseContext(ISystemContext systemContext, string name) 
			: this(systemContext, CreateSimpleConfig(systemContext, name)) {
		}

		public DatabaseContext(ISystemContext systemContext, IConfiguration configuration)
			: base(systemContext) {
			if (systemContext == null)
				throw new ArgumentNullException("systemContext");
			if (configuration == null)
				throw new ArgumentNullException("configuration");

			ContextScope.Unregister<IConfiguration>();
			ContextScope.RegisterInstance<IConfiguration>(configuration);
			ContextScope.RegisterInstance<IDatabaseContext>(this);

			SystemContext = systemContext;

			Configuration = configuration;
			Locker = new Locker(this);

			Sessions = new ActiveSessionList(this);

			InitStorageSystem();
		}

		protected override string ContextName {
			get { return ContextNames.Database; }
		}

		private static IConfiguration CreateSimpleConfig(ISystemContext systemContext, string dbName) {
			if (String.IsNullOrEmpty(dbName))
				throw new ArgumentNullException("dbName");

			var config = new Configuration.Configuration(systemContext.Configuration);
			config.SetValue("database.name", dbName);
			return config;
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (StoreSystem != null)
					StoreSystem.Dispose();

				if (Locker != null)
					Locker.Reset();
			}

			Locker = null;
			StoreSystem = null;

			base.Dispose(disposing);
		}

		public IConfiguration Configuration { get; private set; }

		public ActiveSessionList Sessions { get; private set; }

		public ISystemContext SystemContext { get; private set; }

		public IStoreSystem StoreSystem { get; private set; }

		public Locker Locker { get; private set; }

		private void InitStorageSystem() {
			try {
				var storageTypeName = Configuration.GetString("database.storageSystem", DefaultStorageSystemNames.Heap);
				StoreSystem = this.ResolveService<IStoreSystem>(storageTypeName);

				if (StoreSystem == null)
					throw new DatabaseConfigurationException("The storage system for the database was not set.");
			} catch(DatabaseConfigurationException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("Could not initialize the storage system", ex);
			}
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

	    ITransactionContext IDatabaseContext.CreateTransactionContext() {
	        return CreateTransactionContext();
	    }

	    public TransactionContext CreateTransactionContext() {
	        return new TransactionContext(this);
	    }
	}
}
