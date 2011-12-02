// 
//  Copyright 2010  Deveel
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

using System;
using System.Collections;
using System.Reflection;

using Deveel.Data.Mapping;
using Deveel.Data.Text;

namespace Deveel.Data {
	public sealed class MappingContext {
		/// <summary>
		/// A dictionary handling the mappings for the types specified.
		/// </summary>
		private static readonly Hashtable typeMappings = new Hashtable();

		public static TypeMapping GetTypeMapping(Type type) {
			TypeMapping mapping = typeMappings[type] as TypeMapping;
			if (mapping == null) {
				mapping = TypeMapping.FromType(type);
				typeMappings[type] = mapping;
			}

			return mapping;
		}

		public static Type GetType(string typeString) {
			//TODO:
			return Type.GetType(typeString, true, true);
		}

		private static UserType CreateUserType(UserType parentType, Type type, TableName name) {
			throw new NotImplementedException();
		}

		public static UserType CreateUserType(Type type, TableName name) {
			if (type == null)
				throw new ArgumentNullException("type");

			if (type.IsInterface)
				throw new NotSupportedException("Interface types are not supported (yet).");

			UserType parentType = null;
			Type baseType = type.BaseType;
			while (baseType != null) {
				if (!baseType.IsInterface)
					parentType = CreateUserType(parentType, baseType, null);

				baseType = baseType.BaseType;
			}

			return CreateUserType(parentType, type, name);
		}
	}
}