// 
//  Copyright 2010-2018 Deveel
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

using DryIoc;

namespace Deveel.Data.Services {
	public class ServiceContainer : IServiceContainer {
		private IContainer container;
		private IResolverContext resolver;
		private string scopeName;

		public ServiceContainer()
			: this(null, null) {
		}

		private ServiceContainer(ServiceContainer parent, string scopeName) {
			if (parent != null) {
				resolver = parent.resolver.OpenScope(scopeName, true);
			} else {
				resolver = container = new Container(Rules.Default
					.WithConcreteTypeDynamicRegistrations()
					.WithTrackingDisposableTransients()
					.WithDefaultIfAlreadyRegistered(IfAlreadyRegistered.AppendNotKeyed));
			}

			this.scopeName = scopeName;
		}

		~ServiceContainer() {
			Dispose(false);
		}

		string IScope.Name => scopeName;

		private bool IsScoped => !String.IsNullOrEmpty(scopeName);

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				lock (this) {
					resolver?.Dispose();
					container?.Dispose();
				}
			}

			lock (this) {
				resolver = null;
				container = null;
			}
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		public IScope OpenScope(string name) {
			lock (this) {
				return new ServiceContainer(this, name);
			}
		}

		public void Register(ServiceRegistration registration) {
			if (registration == null)
				throw new ArgumentNullException(nameof(registration));

			if (!IsScoped && container == null)
				throw new InvalidOperationException("The container was not initialized.");
			
			if (IsScoped && registration.Instance == null)
				throw new ArgumentException("Cannot register a service in a scope");

			try {
				lock (this) {

					var serviceType = registration.ServiceType;
					var service = registration.Instance;
					var serviceName = registration.ServiceKey;
					var implementationType = registration.ImplementationType;

					var reuse = Reuse.Transient;

					if (!String.IsNullOrEmpty(registration.Scope)) {
						reuse = Reuse.InCurrentNamedScope(registration.Scope);
					} else if (!String.IsNullOrEmpty(scopeName)) {
						reuse = Reuse.InCurrentNamedScope(scopeName);
					}

					if (service == null) {
						container.Register(serviceType, implementationType, serviceKey: serviceName, reuse: reuse);
					} else if (!String.IsNullOrEmpty(scopeName)) {
						resolver.UseInstance(serviceType, service, serviceKey: serviceName);
					} else {
						container.UseInstance(serviceType, service, serviceKey: serviceName);
					}
				}
			} catch (ServiceException) {
				throw;
			} catch (Exception ex) {
				throw new ServiceException("Error when registering service.", ex);
			}
		}

		public bool Unregister(Type serviceType, object serviceName) {
			if (serviceType == null)
				throw new ArgumentNullException(nameof(serviceType));
			if (IsScoped)
				throw new NotSupportedException("Cannot unregister services from scopes");

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				try {
					container.Unregister(serviceType, serviceName);

					return true;
				} catch (Exception ex) {
					throw new ServiceException("Error when unregistering service", ex);
				}
			}
		}

		public bool IsRegistered(Type serviceType, object key) {
			if (serviceType == null)
				throw new ArgumentNullException(nameof(serviceType));

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				return container.IsRegistered(serviceType, key);
			}
		}

		public object Resolve(Type serviceType, object name) {
			if (serviceType == null)
				throw new ArgumentNullException(nameof(serviceType));

			if (resolver == null)
				throw new InvalidOperationException("The resolver was not initialized.");

			lock (this) {
				try {
					return resolver.Resolve(serviceType, name, IfUnresolved.ReturnDefaultIfNotRegistered);
				} catch (Exception ex) {
					throw new ServiceResolutionException(serviceType, "Error when resolving service", ex);
				}
			}
		}

		public IEnumerable ResolveAll(Type serviceType) {
			if (serviceType == null)
				throw new ArgumentNullException(nameof(serviceType));

			if (resolver == null)
				throw new InvalidOperationException("The resolver was not initialized.");

			lock (this) {
				try {
					return resolver.ResolveMany<object>(serviceType);
				} catch (NullReferenceException) {
					// this means that the container is out of sync in the dispose
					return new object[0];
				} catch (ServiceResolutionException) {
					throw;
				} catch (Exception ex) {
					throw new ServiceResolutionException(serviceType, "Error resolving all services", ex);
				}
			}
		}

		object IServiceProvider.GetService(Type serviceType) {
			return Resolve(serviceType, null);
		}
	}
}
