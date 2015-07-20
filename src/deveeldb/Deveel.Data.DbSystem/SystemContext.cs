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
using System.Linq;

using Deveel.Data.Configuration;
using Deveel.Data.Diagnostics;
using Deveel.Data.Spatial;

namespace Deveel.Data.DbSystem {
	public sealed class SystemContext : ISystemContext, IServiceResolveContext {
		private ISpatialContext spatialContext;
 
		public SystemContext()
			: this(DbConfig.Default) {
		}

		public SystemContext(IDbConfig configuration) {
			if (configuration == null)
				throw new ArgumentNullException("configuration");

			Configuration = configuration;
			EventRegistry = new SystemEventRegistry(this);

			Init();
		}

		~SystemContext() {
			Dispose(false);
		}

		public IDbConfig Configuration { get; private set; }

		public IEventRegistry EventRegistry { get; private set; }

		public ISystemServiceProvider ServiceProvider { get; set; }

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
			RegisterServicesFromConfig();
		}

		private void RegisterServicesFromConfig() {
			var allKeys = Configuration.GetKeys(ConfigurationLevel.Deep);
			var typeKeys = allKeys.Where(key => key.ValueType == typeof (Type));
			var types = typeKeys.Select(key => Configuration.GetValue(key).Value).Cast<Type>();

			foreach (var type in types) {
				ServiceProvider.Register(type);
			}
		}

		object IServiceResolveContext.OnResolve(Type type, string name) {
			if (typeof (ISpatialContext) == type)
				return spatialContext;

			return null;
		}

		void IServiceResolveContext.OnResolved(Type type, string name, object obj) {
			if (type == typeof (ISpatialContext))
				spatialContext = (ISpatialContext) obj;

			if (obj != null && obj is IConfigurable)
				((IConfigurable)obj).Configure(Configuration);
		}

		IEnumerable IServiceResolveContext.OnResolveAll(Type type) {
			return null;
		}

		void IServiceResolveContext.OnResolvedAll(Type type, IEnumerable list) {
			if (list != null) {
				foreach (var service in list.OfType<IConfigurable>()) {
					service.Configure(Configuration);
				}
			}
		}
	}
}
