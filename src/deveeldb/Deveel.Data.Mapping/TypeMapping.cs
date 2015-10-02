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

using Deveel.Data.Types;

namespace Deveel.Data.Mapping {
	public sealed class TypeMapping {
		public TypeMapping(Type type, string tableName) {
			if (tableName == null)
				throw new ArgumentNullException("tableName");

			Type = type;
			TableName = tableName;
			Members = new List<MemberMapping>();
		}

		public Type Type { get; private set; }

		public string TableName { get; private set; }

		public ICollection<MemberMapping> Members { get; private set; }

		public static TypeMapping CreateFor(TypeMappingRequest request) {
			if (request == null)
				throw new ArgumentNullException("request");

			var type = request.Type;

			var tableName = FindTableName(type, request.MappingContext);
			var members = FindMembers(type, request.MappingContext, request.TypeResolver);

			var mapping = new TypeMapping(type, tableName);
			foreach (var member in members) {
				mapping.Members.Add(member);
			}

			return mapping;
		}

		private static IEnumerable<MemberMapping> FindMembers(Type type, ITypeMappingContext mappingContext, ITypeResolver typeResolver) {
			throw new NotImplementedException();
		}

		private static string FindTableName(Type type, ITypeMappingContext mappingContext) {
			throw new NotImplementedException();
		}
	}
}
