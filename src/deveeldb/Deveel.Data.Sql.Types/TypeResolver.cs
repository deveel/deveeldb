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

namespace Deveel.Data.Sql.Types {
	static class TypeResolver {
		public static SqlType Resolve(SqlTypeCode typeCode, string typeName, DataTypeMeta[] metadata, ITypeResolver resolver) {
			if (PrimitiveTypes.IsPrimitive(typeCode))
				return PrimitiveTypes.Resolve(typeCode, typeName, metadata);

			if (resolver == null)
				throw new NotSupportedException(String.Format("Cannot resolve type '{0}' without context.", typeName));

			var resolveCcontext = new TypeResolveContext(typeCode, typeName, metadata);
			return resolver.ResolveType(resolveCcontext);
		}
	}
}
