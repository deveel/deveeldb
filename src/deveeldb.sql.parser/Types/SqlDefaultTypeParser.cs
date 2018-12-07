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
using Deveel.Data.Sql.Parsing;

namespace Deveel.Data.Sql.Types {
	class SqlDefaultTypeParser : ISqlTypeParser {
		public SqlType Parse(IContext context, string s) {
			var typeInfo = PlSqlParser.ParseType(s);

			if (PrimitiveTypes.IsPrimitive(typeInfo.TypeName))
				return PrimitiveTypes.Resolver.Resolve(typeInfo);

			if (context == null)
				throw new Exception($"Type {typeInfo.TypeName} is not primitive and no context is provided");

			var resolver = context.Scope.Resolve<ISqlTypeResolver>();

			if (resolver == null)
				throw new InvalidOperationException($"The type {typeInfo.TypeName} is not primitive and no resolver was found in context");

			return resolver.Resolve(typeInfo);
		}
	}
}