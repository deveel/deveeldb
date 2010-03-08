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
using System.Reflection;

namespace Deveel.Data.Mapping {
	public sealed class TypeAttributeMapping : TypeMemberMapping {
		internal TypeAttributeMapping(TypeMapping declaringType, string memberName, TType type, bool nullable)
			: base(declaringType, memberName) {
			this.type = type;
			this.nullable = nullable;
		}

		private TType type;
		private bool nullable;

		public TType Type {
			get { return type; }
			set {
				if (value == null)
					throw new ArgumentNullException("value");

				type = value;
			}
		}

		internal override MemberTypes MemberType {
			get { return MemberTypes.Attribute; }
		}

		public bool IsNullable {
			get { return nullable; }
			set { nullable = value; }
		}

		private static SqlType GetSqlType(MemberInfo memberInfo) {
			Type memberType;
			if (memberInfo is FieldInfo)
				memberType = ((FieldInfo)memberInfo).FieldType;
			else
				memberType = ((PropertyInfo) memberInfo).PropertyType;

			return GetSqlTypeFromType(memberType);
		}

		internal static TypeAttributeMapping FromMember(TypeMapping declaringType, MemberInfo memberInfo) {
			string name = null;
			SqlType sqlType = GetSqlType(memberInfo);
			int size = -1;
			int scale = -1;
			bool nullable = true;

			object[] attrs = memberInfo.GetCustomAttributes(true);
			for (int i = 0; i < attrs.Length; i++) {
				object attr = attrs[i];

				if (attr is ColumnAttribute) {
					ColumnAttribute columnAttr = (ColumnAttribute)attr;
					name = columnAttr.ColumnName;
					sqlType = columnAttr.SqlType;
					size = columnAttr.Size;
					scale = columnAttr.Scale;
				} else if (attr is NotNullAttribute) {
					nullable = false;
				}
			}

			TType type = GetTType(sqlType, size, scale);

			return new TypeAttributeMapping(declaringType, name, type, nullable);
		}
	}
}