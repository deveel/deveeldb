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

using Deveel.Data;

namespace Deveel.Data.Types {
	public static class QueryContextExtensions {
		public static SqlType ResolveType(this IQueryContext context, string typeName, params DataTypeMeta[] meta) {
			var type = context.SystemContext().ResolveType(typeName, meta);
			if (type != null)
				return type;

			var fullTypeName = context.ResolveObjectName(typeName);
			return context.GetUserType(fullTypeName);
		}

		public static ITypeResolver TypeResolver(this IQueryContext context) {
			return new ContextTypeResolver(context);
		}

		#region ContextTypeResolver

		class ContextTypeResolver : ITypeResolver {
			private readonly IQueryContext queryContext;

			public ContextTypeResolver(IQueryContext context) {
				queryContext = context;
			}

			public SqlType ResolveType(TypeResolveContext context) {
				return queryContext.ResolveType(context.TypeName, context.GetMeta());
			}
		}

		#endregion
	}
}
