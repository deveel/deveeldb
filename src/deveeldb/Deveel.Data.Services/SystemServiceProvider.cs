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
using System.Reflection;

using Deveel.Data.Configuration;

using DryIoc;

namespace Deveel.Data.Services {
	/// <summary>
	/// The default implementation of a <see cref="ISystemServiceProvider"/> that
	/// encapsulates an IoC engine to resolve database services.
	/// </summary>
	public sealed class SystemServiceProvider : ISystemServiceProvider {
		// private TinyIoCContainer container;
		private DryIoc.Container container;
		// private List<IServiceResolveContext> resolveContexts;

		/// <summary>
		/// Constructs the service provider around the given context.
		/// </summary>
		/// <param name="context">The system context in which this service provider
		/// will be constructed around.</param>
		public SystemServiceProvider(ISystemContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			SystemContext = context;

			container = new Container();
			container.RegisterInstance<ISystemContext>(context);
		}

		~SystemServiceProvider() {
			Dispose(false);
		}

		/// <summary>
		/// Gets the system context on top of which this provider is constructed.
		/// </summary>
		/// <seealso cref="ISystemContext.ServiceProvider"/>
		public ISystemContext SystemContext { get; private set; }

		private void Configure(object resolved, IConfigurationProvider provider) {
			if (provider == null)
				provider = SystemContext;

			var configurable = resolved as IConfigurable;
			if (configurable != null &&
				!configurable.IsConfigured)
				configurable.Configure(provider.Configuration);
		}

		private void Configure(IEnumerable list, IConfigurationProvider provider) {
			if (list == null || provider == null)
				return;

			var configurables = list.OfType<IConfigurable>();
			foreach (var configurable in configurables) {
				Configure(configurable, provider);
			}
		}

		private void OnResolved(object resolved, IResolveScope scope) {
			var provider = scope as IConfigurationProvider;
			Configure(resolved, provider);
		}

		private void OnResolved(IEnumerable list, IResolveScope scope) {
			var provider = scope as IConfigurationProvider;
			Configure(list, provider);
		}

		public object Resolve(Type serviceType, string name, IResolveScope scope) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				object resolved = null;
				if (scope != null)
					resolved = scope.OnBeforeResolve(serviceType, name);

				if (resolved == null)
					resolved = container.Resolve(serviceType, name, IfUnresolved.ReturnDefault);

				if (resolved != null && scope != null)
					scope.OnAfterResolve(serviceType, name, resolved);

				OnResolved(resolved, scope);

				return resolved;				
			}
		}

		object IServiceProvider.GetService(Type serviceType) {
			return Resolve(serviceType, null, SystemContext as IResolveScope);
		}

		public IEnumerable ResolveAll(Type serviceType, IResolveScope scope) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				IEnumerable list = null;

				if (scope != null)
					list = scope.OnBeforeResolveAll(serviceType);

				if (list == null) {
					var resolveType = typeof (IEnumerable<>).MakeGenericType(serviceType);
					list = container.Resolve(resolveType, IfUnresolved.ReturnDefault) as IEnumerable;
				}

				if (list != null && scope != null)
					scope.OnAfterResolveAll(serviceType, list);

				OnResolved(list, scope);

				return list;				
			}
		}

		public void Register(string name, Type serviceType, object service) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				if (!serviceType.IsAbstract && !serviceType.IsInterface) {
					var ifaces = serviceType.GetInterfaces();
					foreach (var iface in ifaces) {
						container.Register(iface, serviceType, serviceKey: name, setup:Setup.With(allowDisposableTransient:true));
					}
				}

				if (service == null) {
					container.Register(serviceType, serviceKey: name, setup:Setup.With(allowDisposableTransient:true));
				} else {
					container.RegisterInstance(serviceType, service, serviceKey: name);
				}
			}
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				lock (this) {
					container.Dispose();
				}
			}

			container = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}
