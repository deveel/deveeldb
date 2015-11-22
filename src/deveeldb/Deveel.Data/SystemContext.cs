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
using Deveel.Data.Services;
#if !PCL
using Deveel.Data.Store.Journaled;
#endif

namespace Deveel.Data {
	/// <summary>
	/// This is the context of a database system, that handles the configurations
	/// and services used by all the databases managed within this scope.
	/// </summary>
	public sealed class SystemContext : Context, ISystemContext {
		private IScope container;

		internal SystemContext(IConfiguration configuration, ServiceContainer container) {
			if (configuration == null)
				throw new ArgumentNullException("configuration");

			Configuration = configuration;

			this.container = container.OpenScope(ContextNames.System);
			container.RegisterInstance<IConfiguration>(configuration);
			container.RegisterInstance<ISystemContext>(this);
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

		public DatabaseContext CreateDatabaseContext(IConfiguration configuration) {
			return new DatabaseContext(this, configuration);
		}

		IDatabaseContext ISystemContext.CreateDatabaseContext(IConfiguration configuration) {
			var dbConfig = Configuration.MergeWith(configuration);
			return CreateDatabaseContext(dbConfig);
		}

		protected override void Dispose(bool disposing) {
			if (disposing) {
				if (container != null)
					container.Dispose();
			}

			container = null;
			base.Dispose(true);
		}
	}
}
