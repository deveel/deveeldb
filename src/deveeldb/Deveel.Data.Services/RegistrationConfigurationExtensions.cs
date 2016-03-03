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

namespace Deveel.Data.Services {
	public static class RegistrationConfigurationExtensions {
		public static IRegistrationWithBindingConfiguration<TService, TImplementation> InSystemScope
			<TService, TImplementation>(this IRegistrationWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.System);
		}

		public static IRegistrationWithBindingConfiguration<TService, TImplementation> InDatabaseScope
			<TService, TImplementation>(this IRegistrationWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Database);
		}

		public static IRegistrationWithBindingConfiguration<TService, TImplementation> InQueryScope
			<TService, TImplementation>(this IRegistrationWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Query);
		}

		public static IRegistrationWithBindingConfiguration<TService, TImplementation> InSessionScope
			<TService, TImplementation>(this IRegistrationWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Session);
		}

		public static IRegistrationWithBindingConfiguration<TService, TImplementation> InTransactionScope
			<TService, TImplementation>(this IRegistrationWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Transaction);
		}

		public static IRegistrationWithBindingConfiguration<TService, TImplementation> InBlockScope
			<TService, TImplementation>(this IRegistrationWithBindingConfiguration<TService, TImplementation> configuration)
			where TImplementation : class, TService {
			return configuration.InScope(ContextNames.Block);
		}

		public static IRegistrationWithBindingConfiguration<TService, TService> ToSelf<TService>(this IRegistrationConfiguration<TService> configuration)
			where TService : class {
			return configuration.To<TService>();
		}
	}
}
