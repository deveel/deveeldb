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

namespace Deveel.Data.Build {
	public static class ServiceUseConfigurationExtensions {
		public static IServiceUseWithBindingConfiguration<TService, TService> ToSelf<TService>(this
			IServiceUseConfiguration<TService> configuration)
			where TService : class {
			return configuration.With<TService>();
		}

		public static IServiceUseWithBindingConfiguration<TService, TImplementation> InSystemScope<TService, TImplementation>(
			this IServiceUseWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.System);
		}

		public static IServiceUseWithBindingConfiguration<TService, TImplementation> InTransactionScope
			<TService, TImplementation>(
				this IServiceUseWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Transaction);
		}

		public static IServiceUseWithBindingConfiguration<TService, TImplementation> InDatabaseScope
			<TService, TImplementation>(
				this IServiceUseWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Database);
		}

		public static IServiceUseWithBindingConfiguration<TService, TImplementation> InQueryScope<TService, TImplementation>(
			this IServiceUseWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Query);
		}

		public static IServiceUseWithBindingConfiguration<TService, TImplementation> InSessionScope<TService, TImplementation>(
	this IServiceUseWithBindingConfiguration<TService, TImplementation> configuration)
	where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Session);
		}
	}
}