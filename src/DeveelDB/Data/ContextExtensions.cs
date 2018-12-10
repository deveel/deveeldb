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
using System.Collections.Generic;

using Deveel.Data.Services;

namespace Deveel.Data {
	public static class ContextExtensions {
		#region Internal Helpers

		internal static ISession Session(this IContext context) {
			var parent = context;

			while (parent != null) {
				if (parent is ISession)
					return (ISession) parent;

				parent = parent.ParentContext;
			}

			return null;
		}

		#endregion
		public static object GetService(this IContext context, Type serviceType) {
			return context.Scope.GetService(serviceType);
		}

		public static T GetService<T>(this IContext context) {
			return context.Scope.Resolve<T>();
		}

		public static IEnumerable<T> GetServices<T>(this IContext context) {
			return context.Scope.ResolveAll<T>();
		}

		public static IEnumerable GetServices(this IContext context, Type serviceType) {
			return context.Scope.ResolveAll(serviceType);
		}

	}
}