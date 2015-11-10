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
using System.Collections;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;
using Deveel.Data.Routines;
using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Query;

namespace Deveel.Data {
	/// <summary>
	/// This is the context of a database system, that handles the configurations
	/// and services used by all the databases managed within this scope.
	/// </summary>
	public sealed class SystemContext : ISystemContext /*, IServiceResolveContext */ {
		// We preserve an instance of some singletons...
		private ISqlCompiler sqlCompiler;
		private IEnumerable<IEventRouter> eventRouters;
		private IQueryPlanner queryPlanner;

		/// <summary>
		/// Initializes a new instance of the <see cref="SystemContext"/> class,
		/// using the default set of configurations
		/// </summary>
		public SystemContext()
			: this(Data.Configuration.Configuration.SystemDefault) {
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

			Init();
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

		/// <summary>
		/// Gets an instance of <see cref="IEventRegistry" /> that handles
		/// events happening within the context of the system.
		/// </summary>
		public IEventRegistry EventRegistry { get; private set; }

		/// <summary>
		/// Gets an object used to dynamically resolve system services.
		/// </summary>
		/// <seealso cref="ISystemServiceProvider" />
		public ISystemServiceProvider ServiceProvider { get; set; }

		IEnumerable<KeyValuePair<string, object>> IEventSource.Metadata {
			get { return new KeyValuePair<string, object>[0]; }
		}

		IEventSource IEventSource.ParentSource {
			get { return null; }
		}

		/// <summary>
		/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
		/// </summary>
		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (ServiceProvider != null)
					ServiceProvider.Dispose();
			}

			ServiceProvider = null;
		}

		private void Init() {
			ServiceProvider = new SystemServiceProvider(this);
			this.UseDefaultSqlCompiler();
			this.UseDefaultQueryPlanner();
			this.UseDefaultTableCellCache();
			this.UseSystemFunctions();

			// ServiceProvider.AttachContext(this);
		}

		/*
		object IServiceResolveContext.OnResolve(Type type, string name) {
			if (typeof (ISqlCompiler).IsAssignableFrom(type))
				return sqlCompiler;
			if (typeof (IQueryPlanner).IsAssignableFrom(type))
				return queryPlanner;

			return null;
		}

		void IServiceResolveContext.OnResolved(Type type, string name, object obj) {
			if (obj is ISqlCompiler) {
				sqlCompiler = (ISqlCompiler) obj;
			} else if (obj is IQueryPlanner) {
				queryPlanner = (IQueryPlanner) obj;
			}

			if (obj != null && obj is IConfigurable)
				((IConfigurable)obj).Configure(Configuration);
		}

		IEnumerable IServiceResolveContext.OnResolveAll(Type type) {
			if (type == typeof (IEventRouter))
				return eventRouters;

			return null;
		}

		void IServiceResolveContext.OnResolvedAll(Type type, IEnumerable list) {
			if (type == typeof (IEventRouter))
				eventRouters = list.Cast<IEventRouter>().ToList();

			if (list != null) {
				foreach (var service in list.OfType<IConfigurable>()) {
					service.Configure(Configuration);
				}
			}
		}
		*/
	}
}
