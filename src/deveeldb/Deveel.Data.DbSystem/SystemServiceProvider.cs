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

using TinyIoC;

namespace Deveel.Data.DbSystem {
	/// <summary>
	/// The default implementation of a <see cref="ISystemServiceProvider"/> that
	/// encapsulates an IoC engine to resolve database services.
	/// </summary>
	public sealed class SystemServiceProvider : ISystemServiceProvider {
		private TinyIoCContainer container;

		/// <summary>
		/// Constructs the service provider around the given context.
		/// </summary>
		/// <param name="context">The system context in which this service provider
		/// will be constructed around.</param>
		public SystemServiceProvider(ISystemContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			SystemContext = context;

			container = new TinyIoCContainer();
			container.Register<ISystemContext>(context);
		}

		~SystemServiceProvider() {
			Dispose(false);
		}

		/// <summary>
		/// Gets the system context on top of which this provider is constructed.
		/// </summary>
		/// <seealso cref="ISystemContext.ServiceProvider"/>
		public ISystemContext SystemContext { get; private set; }

		private IServiceResolveContext ResolveContext {
			get { return SystemContext as IServiceResolveContext; }
		}

		public object Resolve(Type serviceType, string name) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (container == null)
				throw new SystemException("The container was not initialized.");

			lock (this) {
				object resolved;
				if (ResolveContext != null) {
					resolved = ResolveContext.OnResolve(serviceType, name);
					if (resolved != null)
						return resolved;
				}

				resolved = container.Resolve(serviceType, name,
					new ResolveOptions { NamedResolutionFailureAction = NamedResolutionFailureActions.AttemptUnnamedResolution });

				if (ResolveContext != null)
					ResolveContext.OnResolved(serviceType, name, resolved);

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
				throw new SystemException("The container was not initialized.");

			lock (this) {
				IEnumerable list;
				if (ResolveContext != null) {
					list = ResolveContext.OnResolveAll(serviceType);
					if (list != null)
						return list;
				}

				list = container.ResolveAll(serviceType, true);

				if (ResolveContext != null) {
					ResolveContext.OnResolvedAll(serviceType, list);
				}

				return list;				
			}
		}

		public void Register(string name, Type serviceType) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (container == null)
				throw new SystemException("The container was not initialized.");

			lock (this) {
				var interfaces = serviceType.GetInterfaces();
				foreach (var iface in interfaces) {
					container.RegisterMultiple(iface, new []{serviceType});
				}

				container.Register(serviceType, name).AsSingleton();
			}
		}

		public void Register(string name, object service) {
			if (service == null)
				return;

			if (container == null)
				throw new SystemException("The container was not initialized.");

			lock (this) {
				var interfaces = service.GetType().GetInterfaces();
				foreach (var iface in interfaces) {
					container.Register(iface, service);
				}

				container.Register(service, name);
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
