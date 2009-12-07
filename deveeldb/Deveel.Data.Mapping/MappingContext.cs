//  
//  MappingContext.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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