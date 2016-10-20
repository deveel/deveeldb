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
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

using DryIoc;

namespace Deveel.Data.Services {
	public static class ScopeExtensions {
		public static object Resolve(this IScope scope, Type serviceType) {
			return scope.Resolve(serviceType, null);
		}

		public static TService Resolve<TService>(this IScope scope, object serviceKey) {
			return (TService) scope.Resolve(typeof(TService), serviceKey);
		}

		public static TService Resolve<TService>(this IScope scope) {
			return scope.Resolve<TService>(null);
		}

		public static IEnumerable<TService> ResolveAll<TService>(this IScope scope) {
			if (scope == null)
				return new TService[0];

			return scope.ResolveAll(typeof (TService)).Cast<TService>();
		}

		public static void Register(this IScope scope, Type serviceType, Type implementationType, object serviceKey) {
			var registration = new ServiceRegistration(serviceType, implementationType);
			registration.ServiceKey = serviceKey;
			scope.Register(registration);
		}

		public static void Register(this IScope scope, Type serviceType) {
			Register(scope, serviceType, null);
		}

		public static void Register(this IScope scope, Type serviceType, object serviceKey) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");

			if (serviceType.IsValueType())
				throw new ArgumentException(String.Format("The service type '{0}' to register is not a class.", serviceType));

			var interfaces = serviceType.GetImplementedInterfaces();
			foreach (var interfaceType in interfaces) {
				scope.Register(interfaceType, serviceType, serviceKey);
			}

			scope.Register(serviceType, serviceType, serviceKey);
		}

		public static void Register<TService, TImplementation>(this IScope scope, object serviceKey)
			where TImplementation : class, TService {
			scope.Register(typeof(TService), typeof(TImplementation), serviceKey);
		}

		public static void Register<TService, TImplementation>(this IScope scope)
			where TImplementation : class, TService {
			scope.Register<TService, TImplementation>(null);
		}

		public static void Register<TService>(this IScope scope, object serviceKey)
			where TService : class {
			scope.Register(typeof(TService), serviceKey);
		}

		public static void Register<TService>(this IScope scope)
			where TService : class {
			scope.Register<TService>(null);
		}

		public static void RegisterInstance(this IScope scope, Type serviceType, object instance) {
			RegisterInstance(scope, serviceType, instance, null);
		}

		public static void RegisterInstance(this IScope scope, Type serviceType, object instance, object serviceKey) {
			if (serviceType == null)
				throw new ArgumentNullException("serviceType");
			if (instance == null)
				throw new ArgumentNullException("instance");

			var implementationType = instance.GetType();
			var registration = new ServiceRegistration(serviceType, implementationType);
			registration.Instance = instance;
			registration.ServiceKey = serviceKey;
			scope.Register(registration);
		}

		public static void RegisterInstance<TService>(this IScope scope, TService instance) where TService : class {
			RegisterInstance<TService>(scope, instance, null);
		}

		public static void RegisterInstance<TService>(this IScope scope, TService instance, object serviceKey) where TService : class {
			var interfaces = typeof (TService).GetImplementedInterfaces();
			foreach (var interfaceType in interfaces) {
				scope.RegisterInstance(interfaceType, instance, serviceKey);
			}

			scope.RegisterInstance(typeof(TService), instance, serviceKey);
		}

		public static bool Unregister(this IScope scope, Type serviceType) {
			return scope.Unregister(serviceType, null);
		}

		public static bool Unregister<TService>(this IScope scope, object serviceKey) {
			return scope.Unregister(typeof (TService), serviceKey);
		}

		public static bool Unregister<TService>(this IScope scope) {
			return scope.Unregister<TService>(null);
		}

		public static void Replace(this IScope scope, Type serviceType, Type implementationType) {
			scope.Replace(serviceType, implementationType, null);
		}

		public static void Replace(this IScope scope, Type serviceType, Type implementationType, object serviceKey) {
			if (scope.Unregister(serviceType, serviceKey))
				scope.Register(serviceType, implementationType, serviceKey);
		}

		public static void Replace<TService, TImplementation>(this IScope scope)
			where TImplementation : class, TService {
			scope.Replace<TService, TImplementation>(null);
		}

		public static void Replace<TService, TImplementation>(this IScope scope, object serviceKey)
			where TImplementation : class, TService {
			scope.Replace(typeof(TService), typeof(TImplementation), serviceKey);
		}

		public static bool IsRegistered(this IScope scope, Type serviceType) {
			return scope.IsRegistered(serviceType, null);
		}

		public static bool IsRegistered<T>(this IScope scope, object serviceKey) {
			return scope.IsRegistered(typeof(T), serviceKey);
		}

		public static bool IsRegistered<T>(this IScope scope) {
			return scope.IsRegistered<T>(null);
		}
	}
}
