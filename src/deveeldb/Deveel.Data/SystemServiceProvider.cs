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

using DryIoc;

namespace Deveel.Data {
	/// <summary>
	/// The default implementation of a <see cref="ISystemServiceProvider"/> that
	/// encapsulates an IoC engine to resolve database services.
	/// </summary>
	public sealed class SystemServiceProvider : ISystemServiceProvider {
		// private TinyIoCContainer container;
		private DryIoc.Container container;
		private List<IServiceResolveContext> resolveContexts;

		/// <summary>
		/// Constructs the service provider around the given context.
		/// </summary>
		/// <param name="context">The system context in which this service provider
		/// will be constructed around.</param>
		public SystemServiceProvider(ISystemContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			SystemContext = context;

			container = new DryIoc.Container();
			container.RegisterInstance<ISystemContext>(context);

			resolveContexts = new List<IServiceResolveContext>();
		}

		~SystemServiceProvider() {
			Dispose(false);
		}

		/// <summary>
		/// Gets the system context on top of which this provider is constructed.
		/// </summary>
		/// <seealso cref="ISystemContext.ServiceProvider"/>
		public ISystemContext SystemContext { get; private set; }

		private bool HasAnyContext {
			get { return resolveContexts.Count > 0; }
		}

		private object OnResolve(Type serviceType, string name) {
			return resolveContexts.Select(context => context.OnResolve(serviceType, name))
				.FirstOrDefault(resolved => resolved != null);
		}

		private void OnResolved(Type serviceType, string name, object instance) {
			foreach (var context in resolveContexts) {
				context.OnResolved(serviceType, name, instance);
			}
		}

		private IEnumerable OnResolveAll(Type serviceType) {
			return resolveContexts.Select(context => context.OnResolveAll(serviceType))
				.FirstOrDefault(resolved => resolved != null);
		}

		private void OnResolvedAll(Type serviceType, IEnumerable instances) {
			foreach (var context in resolveContexts) {
				context.OnResolvedAll(serviceType, instances);
			}
		}

		public void AttachContext(IServiceResolveContext resolveContext) {
			if (!resolveContexts.Contains(resolveContext))
				resolveContexts.Add(resolveContext);
		}

		public object Resolve(Type serviceType, string name) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				var resolved = OnResolve(serviceType, name);
					if (resolved != null)
						return resolved;

				resolved = container.Resolve(serviceType, name, IfUnresolved.ReturnDefault);

				OnResolved(serviceType, name, resolved);

				return resolved;				
			}
		}

		object IServiceProvider.GetService(Type serviceType) {
			return Resolve(serviceType, null);
		}

		public IEnumerable ResolveAll(Type serviceType) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				IEnumerable list = OnResolveAll(serviceType);
					if (list != null)
						return list;

				var resolveType = typeof (IEnumerable<>).MakeGenericType(serviceType);
				list = container.Resolve(resolveType) as IEnumerable;

				OnResolvedAll(serviceType, list);

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
						container.Register(iface, serviceType, serviceKey: name, reuse:Reuse.Singleton);
					}
				}

				if (service == null) {
					container.Register(serviceType, serviceKey: name, reuse:Reuse.Singleton);
				} else {
					container.RegisterInstance(serviceType, service, serviceKey: name, reuse:Reuse.Singleton);
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
