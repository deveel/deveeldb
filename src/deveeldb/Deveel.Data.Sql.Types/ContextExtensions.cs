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

using Deveel.Data.Services;

namespace Deveel.Data.Sql.Types {
	public static class ContextExtensions {
		public static SqlType ResolveType(this IContext context, string typeName, params DataTypeMeta[] meta) {
			if (PrimitiveTypes.IsPrimitive(typeName))
				return PrimitiveTypes.Resolve(typeName, meta);

			var resolvers = context.ResolveAllServices<ITypeResolver>();
			foreach (var resolver in resolvers) {
				var sqlType = resolver.ResolveType(new TypeResolveContext(SqlTypeCode.Unknown, typeName, meta));
				if (sqlType != null)
					return sqlType;
			}

			return null;
		}

		public static ITypeResolver TypeResolver(this IContext context) {
			return new ContextTypeResolver(context);
		}

		#region ContextTypeResolver

		class ContextTypeResolver : ITypeResolver {
			private readonly IContext queryContext;

			public ContextTypeResolver(IContext context) {
				queryContext = context;
			}

			public SqlType ResolveType(TypeResolveContext context) {
				return queryContext.ResolveType(context.TypeName, context.GetMeta());
			}
		}

		#endregion
	}
}
