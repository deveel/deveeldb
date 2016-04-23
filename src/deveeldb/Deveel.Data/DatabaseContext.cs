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

using Deveel.Data.Configuration;
using Deveel.Data.Services;
using Deveel.Data.Store;
using Deveel.Data.Transactions;

namespace Deveel.Data {
	/// <summary>
	/// The default implementation of a <see cref="IDatabaseContext"/> that
	/// encapsulates the services and configurations of a database.
	/// </summary>
	/// <remarks>
	/// The <see cref="DatabaseContext"/> is required to open or create
	/// a database: this implies the only responsible for the creation of this
	/// context instance is the parent <see cref="ISystemContext"/>. 
	/// </remarks>
	/// <seealso cref="Database"/>
	/// <seealso cref="IDatabase"/>
	/// <seealso cref="IDatabaseContext"/>
	public sealed class DatabaseContext : Context, IDatabaseContext {
		internal DatabaseContext(ISystemContext systemContext, IConfiguration configuration)
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

			InitStorageSystem();
		}

		protected override string ContextName {
			get { return ContextNames.Database; }
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (StoreSystem != null)
					StoreSystem.Dispose();
			}

			StoreSystem = null;

			base.Dispose(disposing);
		}

		public IConfiguration Configuration { get; private set; }

		public ISystemContext SystemContext { get; private set; }

		public IStoreSystem StoreSystem { get; private set; }

		private void InitStorageSystem() {
			try {
				var storageTypeName = Configuration.GetString("database.storageSystem", DefaultStorageSystemNames.Heap);
				if (String.IsNullOrEmpty(storageTypeName))
					throw new DatabaseConfigurationException("No storage system was configured for this database.");

				StoreSystem = this.ResolveService<IStoreSystem>(storageTypeName);

				if (StoreSystem == null)
					throw new DatabaseConfigurationException(String.Format("The storage system '{0}' for the database was not set.", storageTypeName));
			} catch(DatabaseConfigurationException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseConfigurationException("Could not initialize the storage system", ex);
			}
		}

	    ITransactionContext IDatabaseContext.CreateTransactionContext() {
	        return CreateTransactionContext();
	    }

		/// <summary>
		/// Creates a new context to be provided to a <see cref="Transaction"/>
		/// instance that is handled by the parent database.
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="TransactionContext"/> that is
		/// the foundation context of a <see cref="Transaction"/>.
		/// </returns>
		/// <seealso cref="IDatabaseContext.CreateTransactionContext"/>
		/// <seealso cref="ITransactionContext"/>
		/// <seealso cref="TransactionContext"/>
		/// <seealso cref="Transaction"/>
	    public TransactionContext CreateTransactionContext() {
	        return new TransactionContext(this);
	    }
	}
}
