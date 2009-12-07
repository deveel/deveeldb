//  
//  TypeAttributeMapping.cs
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