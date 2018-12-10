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

using Deveel.Data.Services;

namespace Deveel.Data.Query {
	public static class ContextExtensions {
		#region Internal Helpers

		internal static IReferenceResolver Resolver(this IContext context) {
			var parent = context;

			while (parent != null) {
				if (parent is IQuery)
					return ((IQuery) parent).Resolver;

				parent = parent.ParentContext;
			}

			return null;
		}

		internal static IGroupResolver GroupResolver(this IContext context) {
			var parent = context;

			while (parent != null) {
				if (parent is IQuery)
					return ((IQuery) parent).GroupResolver;

				parent = parent.ParentContext;
			}

			return null;
		}

		#endregion

		public static IQuery CreateQuery(this IContext context) {
			return CreateQuery(context, context.Resolver());
		}

		public static IQuery CreateQuery(this IContext context, IReferenceResolver resolver) {
			return CreateQuery(context, context.GroupResolver(), resolver);
		}

		public static IQuery CreateQuery(this IContext context, IGroupResolver groupResolver, IReferenceResolver resolver) {
			return new QueryImpl(context, groupResolver, resolver);
		}

		#region QueryImpl

		class QueryImpl : Context, IQuery {
			public QueryImpl(IContext parent, IGroupResolver groupResolver, IReferenceResolver resolver) 
				: base(parent, KnownScopes.Query) {
				GroupResolver = groupResolver;
				Resolver = resolver;
			}

			public IGroupResolver GroupResolver { get; }

			public IReferenceResolver Resolver { get; }
		}

		#endregion

	}
}