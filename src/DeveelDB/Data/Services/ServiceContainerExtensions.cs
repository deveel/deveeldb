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

namespace Deveel.Data.Services {
	public static class ServiceContainerExtensions {
		#region Register

		public static IServiceContainer Register(this IServiceContainer container, Type serviceType, Type implementationType, object serviceKey) {
			return Register(container, serviceType, implementationType, null, serviceKey);
		}

		public static IServiceContainer Register(this IServiceContainer container, Type serviceType, Type implementationType, string scope, object serviceKey) {
			container.Register(new ServiceRegistration(serviceType, implementationType) {
				ServiceKey = serviceKey, 
				Scope = scope
			});

			return container;
		}

		public static IServiceContainer Register(this IServiceContainer container, Type serviceType) {
			return Register(container, serviceType, null);
		}

		public static IServiceContainer Register(this IServiceContainer container, Type serviceType, string scope) {
			return Register(container, serviceType, scope, null);
		}

		public static IServiceContainer Register(this IServiceContainer container, Type serviceType, object serviceKey) {
			return Register(container, serviceType, (string)null, serviceKey);
		}

		public static IServiceContainer Register(this IServiceContainer container, Type serviceType, string scope, object serviceKey) {
			if (serviceType == null)
				throw new ArgumentNullException(nameof(serviceType));

			if (serviceType.IsValueType)
				throw new ArgumentException($"The service type '{serviceType}' to register is not a class.");

			return container.Register(serviceType, serviceType, scope, serviceKey);
		}

		public static IServiceContainer Register<TService, TImplementation>(this IServiceContainer container, object serviceKey)
			where TImplementation : class, TService {
			return Register<TService, TImplementation>(container, null, serviceKey);
		}

		public static IServiceContainer Register<TService, TImplementation>(this IServiceContainer container, string scope, object serviceKey)
			where TImplementation : class, TService {
			return container.Register(typeof(TService), typeof(TImplementation), scope, serviceKey);
		}

		public static IServiceContainer Register<TService, TImplementation>(this IServiceContainer container)
			where TImplementation : class, TService {
			return Register<TService, TImplementation>(container, null);
		}

		public static IServiceContainer Register<TService, TImplementation>(this IServiceContainer container, string scope)
			where TImplementation : class, TService {
			return container.Register<TService, TImplementation>(scope, null);
		}

		public static IServiceContainer Register<TService>(this IServiceContainer container, object serviceKey)
			where TService : class {
			return Register<TService>(container, null, serviceKey);
		}

		public static IServiceContainer Register<TService>(this IServiceContainer container, string scope, object serviceKey)
			where TService : class {
			return container.Register(typeof(TService), scope, serviceKey);
		}

		public static IServiceContainer Register<TService>(this IServiceContainer container)
			where TService : class {
			return Register<TService>(container, null);
		}

		public static IServiceContainer Register<TService>(this IServiceContainer container, string scope)
			where TService : class {
			return container.Register<TService>(scope, null);
		}

		#endregion

		public static void RegisterInstance(this IServiceContainer container, Type serviceType, object instance) {
			RegisterInstance(container, serviceType, instance, null);
		}

		public static void RegisterInstance(this IServiceContainer container, Type serviceType, object instance, object serviceKey) {
			if (serviceType == null)
				throw new ArgumentNullException(nameof(serviceType));
			if (instance == null)
				throw new ArgumentNullException(nameof(instance));

			var implementationType = instance.GetType();
			var registration = new ServiceRegistration(serviceType, implementationType);
			registration.Instance = instance;
			if (serviceKey != null)
				registration.ServiceKey = serviceKey;

			container.Register(registration);
		}

		public static void RegisterInstance<TService>(this IServiceContainer container, object instance) {
			container.RegisterInstance<TService>(instance, null);
		}

		public static void RegisterInstance<TService>(this IServiceContainer container, TService instance)
			where TService : class {
			container.RegisterInstance<TService>((object) instance);
		}

		public static void RegisterInstance<TService>(this IServiceContainer container, object instance, object serviceKey) {
			container.RegisterInstance(typeof(TService), instance, serviceKey);
		}

		public static bool Unregister(this IServiceContainer container, Type serviceType) {
			return container.Unregister(serviceType, null);
		}

		public static bool Unregister<TService>(this IServiceContainer container, object serviceKey) {
			return container.Unregister(typeof(TService), serviceKey);
		}

		public static bool Unregister<TService>(this IServiceContainer container) {
			return container.Unregister<TService>(null);
		}

		public static bool Replace(this IServiceContainer container, Type serviceType, Type implementationType) {
			return container.Replace(serviceType, implementationType, null);
		}

		public static bool Replace(this IServiceContainer container, Type serviceType, Type implementationType, object serviceKey) {
			bool replaced = false;
			if (container.IsRegistered(serviceType, serviceKey)) {
				container.Unregister(serviceType, serviceKey);
				replaced = true;
			}

			container.Register(serviceType, implementationType, serviceKey);
			return replaced;
		}

		public static bool Replace<TService, TImplementation>(this IServiceContainer container)
			where TImplementation : class, TService {
			return container.Replace<TService, TImplementation>(null);
		}

		public static bool Replace<TService, TImplementation>(this IServiceContainer container, object serviceKey)
			where TImplementation : class, TService {
			return container.Replace(typeof(TService), typeof(TImplementation), serviceKey);
		}

		public static bool ReplaceInstance(this IServiceContainer container, Type serviceType, object instance) {
			return ReplaceInstance(container, serviceType, instance, null);
		}

		public static bool ReplaceInstance(this IServiceContainer container, Type serviceType, object instance, object serviceKey) {
			bool replaced = false;
			if (container.IsRegistered(serviceType, serviceKey)) {
				container.Unregister(serviceType, serviceKey);
				replaced = true;
			}

			container.RegisterInstance(serviceType, instance, serviceKey);
			return replaced;
		}

		public static bool ReplaceInstance<TService>(this IServiceContainer container, object instance) {
			return ReplaceInstance<TService>(container, instance, null);
		}

		public static bool ReplaceInstance<TService>(this IServiceContainer container, object instance, object serviceKey) {
			return container.ReplaceInstance(typeof(TService), instance, serviceKey);
		}

		public static bool IsRegistered(this IServiceContainer container, Type serviceType) {
			return container.IsRegistered(serviceType, null);
		}

		public static bool IsRegistered<T>(this IServiceContainer container, object serviceKey) {
			return container.IsRegistered(typeof(T), serviceKey);
		}

		public static bool IsRegistered<T>(this IServiceContainer container) {
			return container.IsRegistered<T>(null);
		}
	}
}