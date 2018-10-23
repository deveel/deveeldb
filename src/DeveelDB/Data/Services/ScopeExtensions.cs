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
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Services {
	public static class ScopeExtensions {

		public static object Resolve(this IScope container, Type serviceType) {
			return container.Resolve(serviceType, null);
		}

		public static TService Resolve<TService>(this IScope container, object serviceKey) {
			return (TService) container.Resolve(typeof(TService), serviceKey);
		}

		public static TService Resolve<TService>(this IScope container) {
			return container.Resolve<TService>(null);
		}

		public static IEnumerable<TService> ResolveAll<TService>(this IScope container) {
			if (container == null)
				return new TService[0];

			return container.ResolveAll(typeof (TService)).Cast<TService>();
		}
	}
}

