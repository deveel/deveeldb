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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Configuration;

namespace Deveel.Data {
	public static class SystemServiceProviderExtensions {
		public static object Resolve(this ISystemServiceProvider provider, Type type) {
			return Resolve(provider, type, null);
		}

		public static object Resolve(this ISystemServiceProvider provider, Type type, IConfigurationProvider configurationProvider) {
			return provider.Resolve(type, null, configurationProvider);
		}

		public static T Resolve<T>(this ISystemServiceProvider provider) {
			return Resolve<T>(provider, null,  null);
		}

		public static T Resolve<T>(this ISystemServiceProvider provider, IConfigurationProvider configurationProvider) {
			return Resolve<T>(provider, null, configurationProvider);
		}

		public static T Resolve<T>(this ISystemServiceProvider provider, string name) {
			return Resolve<T>(provider, name, null);
		}

		public static T Resolve<T>(this ISystemServiceProvider provider, string name, IConfigurationProvider configurationProvider) {
			return (T) provider.Resolve(typeof (T), name, configurationProvider);
		}

		public static IEnumerable<T> ResolveAll<T>(this ISystemServiceProvider provider) {
			return ResolveAll<T>(provider, null);
		}

		public static IEnumerable<T> ResolveAll<T>(this ISystemServiceProvider provider, IConfigurationProvider configurationProvider) {
			return provider.ResolveAll(typeof (T), configurationProvider).Cast<T>();
		}

		public static void Register(this ISystemServiceProvider provider, Type type) {
			provider.Register(null, type, null);
		}

		public static void Register<T>(this ISystemServiceProvider provider, string name) {
			provider.Register(name, typeof(T), null);
		}

		public static void Register<T>(this ISystemServiceProvider provider) {
			provider.Register(typeof(T));
		}

		public static void Register(this ISystemServiceProvider provider, object service) {
			if (service == null)
				throw new ArgumentNullException("service");

			var serviceType = service.GetType();
			provider.Register(null, serviceType, service);
		}

		public static void Register<T>(this ISystemServiceProvider provider, T service) {
			provider.Register(null, typeof(T), service);
		}
	}
}
