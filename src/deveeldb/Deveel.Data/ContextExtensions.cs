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
using System.Collections;
using System.Collections.Generic;

using Deveel.Data.Services;

namespace Deveel.Data {
	public static class ContextExtensions {
		public static void DeclareException(this IContext context, int errorCode, string exceptionName) {
			if (SystemErrorCodes.IsSystemError(errorCode))
				throw new ArgumentException(String.Format("The code '{0}' is reserved for system", errorCode));
			if (SystemErrorCodes.IsSystemError(exceptionName))
				throw new ArgumentException(String.Format("The exception name '{0}' is reserved for system", exceptionName));

			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is IExceptionInitScope) {
					var scope = (IExceptionInitScope)currentContext;
					scope.DeclareException(errorCode, exceptionName);
					return;
				}

				currentContext = currentContext.Parent;
			}

			throw new InvalidOperationException("Unable to declare the exception.");
		}

		public static DeclaredException FindDeclaredException(this IContext context, string exceptionName) {
			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is IExceptionInitScope) {
					var scope = (IExceptionInitScope)currentContext;
					var exception = scope.FindExceptionByName(exceptionName);
					if (exception != null)
						return exception;
				}

				currentContext = currentContext.Parent;
			}

			return null;
		}

		public static object ResolveService(this IContext context, Type serviceType, object serviceKey) {
			return context.Scope.Resolve(serviceType, serviceKey);
		}

		public static object ResolveService(this IContext context, Type serviceType) {
			return context.Scope.Resolve(serviceType);
		}

		public static TService ResolveService<TService>(this IContext context, object serviceKey) {
			return context.Scope.Resolve<TService>(serviceKey);
		}

		public static TService ResolveService<TService>(this IContext context) {
			return context.Scope.Resolve<TService>();
		}

		public static IEnumerable ResolveAllServices(this IContext context, Type serviceType) {
			return context.Scope.ResolveAll(serviceType);
		} 

		public static IEnumerable<TService> ResolveAllServices<TService>(this IContext context) {
			return context.Scope.ResolveAll<TService>();
		}

		public static void RegisterService(this IContext context, Type serviceType, Type implementationType, object serviceKey) {
			context.Scope.Register(serviceType, implementationType, serviceKey);
		}

		public static void RegisterService(this IContext context, Type serviceType, Type implementationType) {
			context.Scope.Register(serviceType, implementationType);
		}

		public static void RegisterService(this IContext context, Type serviceType, object serviceKey) {
			context.Scope.Register(serviceType, serviceKey);
		}

		public static void RegisterService(this IContext context, Type serviceType) {
			context.Scope.Register(serviceType);
		}

		public static void RegisterService<TService, TImplementation>(this IContext context, object serviceKey)
			where TImplementation : class, TService {
			context.Scope.Register<TService, TImplementation>(serviceKey);
		}

		public static void RegisterService<TService, TImplementation>(this IContext context)
			where TImplementation : class, TService {
			context.Scope.Register<TService, TImplementation>();
		}

		public static void RegisterService<TService>(this IContext context, object serviceKey)
			where TService : class {
			context.Scope.Register<TService>(serviceKey);
		}

		public static void RegisterService<TService>(this IContext context)
			where TService : class {
			context.Scope.Register<TService>();
		}

		public static void RegisterInstance(this IContext context, Type serviceType, object instance, object serviceKey) {
			context.Scope.RegisterInstance(serviceType, instance, serviceKey);
		}

		public static void RegisterInstance(this IContext context, Type serviceType, object instance) {
			context.Scope.RegisterInstance(serviceType, instance);
		}

		public static void RegisterInstance<TService>(this IContext context, TService instance, object serviceKey)
			where TService : class {
			context.Scope.RegisterInstance<TService>(instance, serviceKey);
		}

		public static void RegisterInstance<TService>(this IContext context, TService instance)
			where TService : class {
			context.Scope.RegisterInstance<TService>(instance);
		}

		public static void UnregisterService(this IContext context, Type serviceType) {
			context.Scope.Unregister(serviceType);
		}

		public static void UnregisterService<TService>(this IContext context, object serviceKey) {
			context.Scope.Unregister<TService>(serviceKey);
		}

		public static void UnregisterService<TService>(this IContext context) {
			context.Scope.Unregister<TService>();
		}
	}
}
