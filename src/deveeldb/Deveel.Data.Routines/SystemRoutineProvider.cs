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

using Deveel.Data.DbSystem;


namespace Deveel.Data.Routines {
	public sealed class SystemRoutineProvider : IRoutineResolverContainer {
		private Dictionary<Type, IRoutineResolver> resolvers; 

		public SystemRoutineProvider(IDatabaseContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			Context = context;
			resolvers = new Dictionary<Type, IRoutineResolver>();
		}

		public IDatabaseContext Context { get; private set; }

		public void RegisterResolver(IRoutineResolver resolver) {
			if (resolver == null)
				return;

			var type = resolver.GetType();
			if (type.IsAbstract || type.IsInterface)
				throw new ArgumentException();

			if (!resolvers.ContainsKey(type))
				resolvers[type] = resolver;
		}

		public IRoutine ResolveRoutine(Invoke request, IQueryContext context) {
			return resolvers.Values.Select(resolver => resolver.ResolveRoutine(request, context))
				.FirstOrDefault(routine => routine != null);
		}
	}
}
