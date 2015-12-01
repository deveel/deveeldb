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

using DryIoc;

namespace Deveel.Data.Services {
	public class ServiceContainer : IScope, IServiceProvider {
		private IContainer container;
		private List<IRegistrationConfigurationProvider> registrationProviders; 

		public ServiceContainer() 
			: this(null, null) {
		}

		private ServiceContainer(ServiceContainer parent, string scopeName) {
			if (parent != null) {
				container = parent.container.OpenScope(scopeName)
					.With(rules => rules.WithDefaultReuseInsteadOfTransient(Reuse.InCurrentNamedScope(scopeName)));

				ScopeName = scopeName;
			} else {
				container = new Container(Rules.Default
					.WithDefaultReuseInsteadOfTransient(Reuse.Singleton)
					.WithoutThrowOnRegisteringDisposableTransient());
			}

			registrationProviders = new List<IRegistrationConfigurationProvider>();
		}

		~ServiceContainer() {
			Dispose(false);
		}

		object IServiceProvider.GetService(Type serviceType) {
			return Resolve(serviceType, null);
		}

		protected virtual void Dispose(bool disposing) {
			if (disposing) {
				lock (this) {
					if (container != null)
						container.Dispose();
				}
			}

			container = null;
		}

		private string ScopeName { get; set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		private void RegisterConfigurations() {
			if (registrationProviders != null && registrationProviders.Count > 0) {
				foreach (var provider in registrationProviders) {
					RegisterConfiguration(provider);
				}

				registrationProviders.Clear();
			}
		}

		private void RegisterConfiguration(IRegistrationConfigurationProvider provider) {
			var registration = new ServiceRegistration(provider.ServiceType, provider.ImplementationType) {
				Scope = provider.ScopeName,
				ServiceKey = provider.ServiceKey,
				Instance = provider.Instance
			};

			Register(registration);
		}

		public IScope OpenScope(string name) {
			RegisterConfigurations();
			return new ServiceContainer(this, name);
		}

		public object Resolve(Type serviceType, object name) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				return container.Resolve(serviceType, name, IfUnresolved.ReturnDefault);
			}
		}

		public IRegistrationConfiguration<TService> Bind<TService>() {
			var config = new RegistrationConfiguration<TService>(this);
			registrationProviders.Add(config);
			return config;
		} 

		public IEnumerable ResolveAll(Type serviceType) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				return container.ResolveMany<object>(serviceType);
			}
		}

		public void Register(ServiceRegistration registration) {
			if (registration == null)
				throw new ArgumentNullException("registration");

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				var serviceType = registration.ServiceType;
				var service = registration.Instance;
				var serviceName = registration.ServiceKey;
				var implementationType = registration.ImplementationType;

				var reuse = Reuse.Singleton;
				if (!String.IsNullOrEmpty(ScopeName))
					reuse = Reuse.InCurrentNamedScope(ScopeName);

				if (!String.IsNullOrEmpty(registration.Scope))
					reuse = Reuse.InCurrentNamedScope(registration.Scope);

				if (service == null) {
					container.Register(serviceType, implementationType, serviceKey: serviceName, reuse: reuse);
				} else {
					container.RegisterInstance(serviceType, service, serviceKey: serviceName, reuse: reuse);
				}
			}
		}

		public bool Unregister(Type serviceType, object serviceName) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (container == null)
				throw new InvalidOperationException("The container was not initialized.");

			lock (this) {
				container.Unregister(serviceType, serviceName);
				return true;
			}
		}

		#region RegistrationConfiguration

		class RegistrationConfiguration<TService> : IRegistrationConfiguration<TService>, IRegistrationConfigurationProvider {
			public RegistrationConfiguration(ServiceContainer container) {
				Container = container;
			}

			public IRegistrationWithBindingConfiguration<TService, TImplementation> To<TImplementation>() where TImplementation : class, TService {
				AssertNotBound();
				ImplementationType = typeof (TImplementation);
				IsBound = true;
				return new RegistrationWithBindingConfiguration<TService, TImplementation>(this);
			}

			public IRegistrationWithBindingConfiguration<TService, TImplementation> ToInstance<TImplementation>(TImplementation instance) where TImplementation : class, TService {
				AssertNotBound();
				ImplementationType = typeof (TImplementation);
				Instance = instance;
				IsBound = true;
				return new RegistrationWithBindingConfiguration<TService, TImplementation>(this);
			}

			public Type ServiceType {
				get { return typeof (TService); }
			}

			private void AssertNotBound() {
				if (IsBound)
					throw new InvalidOperationException("The registration was already bound.");
			}

			public ServiceContainer Container { get; private set; }

			private bool IsBound { get; set; }

			public Type ImplementationType { get; set; }

			public object ServiceKey { get; set; }

			public string ScopeName { get; set; }

			public object Instance { get; private set; }
		}

		#endregion

		#region RegistrationWithBindingConfiguration

		class RegistrationWithBindingConfiguration<TService, TImplementation> : IRegistrationWithBindingConfiguration<TService, TImplementation> {
			private RegistrationConfiguration<TService> configuration;

			public RegistrationWithBindingConfiguration(RegistrationConfiguration<TService> configuration) {
				this.configuration = configuration;
			}

			public IRegistrationWithBindingConfiguration<TService, TImplementation> InScope(string scopeName) {
				configuration.ScopeName = scopeName;
				return this;
			}

			public IRegistrationWithBindingConfiguration<TService, TImplementation> WithKey(object serviceKey) {
				configuration.ServiceKey = serviceKey;
				return this;
			}
		}

		#endregion
	}
}
