// 
//  Copyright 2010-2016 Deveel
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
using System.IO;
using System.Text;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Variables;

namespace Deveel.Data.Sql.Types {
	public sealed class UserType : SqlType, IDbObject {
		public UserType(UserTypeInfo typeInfo) 
			: base(typeInfo.TypeName.FullName, SqlTypeCode.Type) {
			if (typeInfo == null)
				throw new ArgumentNullException("typeInfo");

			TypeInfo = typeInfo;
		}

		public UserTypeInfo TypeInfo { get; private set; }

		IObjectInfo IDbObject.ObjectInfo {
			get { return TypeInfo; }
		}

		public ObjectName FullName {
			get { return TypeInfo.TypeName; }
		}

		public override bool IsComparable(SqlType type) {
			// For the moment not possible to compare
			return false;
		}

		public override bool Equals(SqlType other) {
			if (!(other is UserType))
				return false;

			var otherType = (UserType)other;
			if (!FullName.Equals(otherType.FullName, true))
				return false;

			if (MemberCount != otherType.MemberCount)
				return false;

			for (int i = 0; i < MemberCount; i++) {
				var thisMember = TypeInfo.MemberAt(i);
				var otherMember = otherType.TypeInfo.MemberAt(i);

				if (!thisMember.MemberName.Equals(otherMember.MemberName) ||
				    !thisMember.MemberType.Equals(otherMember.MemberType))
					return false;
			}

			return true;
		}

		public override int GetHashCode() {
			var code = FullName.GetHashCode();

			for (int i = 0; i < MemberCount; i++) {
				var member = TypeInfo.MemberAt(i);

				code ^= member.GetHashCode();
			}

			return code;
		}

		public override string ToString() {
			var sb = new StringBuilder(FullName.FullName);

			sb.Append("(");

			for (int i = 0; i < MemberCount; i++) {
				var member = TypeInfo.MemberAt(i);

				sb.Append(member.MemberName);
				sb.Append(" ");
				sb.Append(member.MemberType);

				if (i < MemberCount - 1)
					sb.Append(", ");
			}

			sb.Append(")");

			return sb.ToString();
		}

		/*
		public override bool CanCastTo(SqlType destType) {
			if (!(destType is UserType))
				return false;

			var otherType = (UserType) destType;
			var memberCount = MemberCount;
			if (!FullName.Equals(otherType.FullName, true) ||
				memberCount != otherType.MemberCount)
				return false;

			return true;
		}

		public override ISqlObject CastTo(ISqlObject value, SqlType destType) {
			if (!(destType is UserType))
				throw new ArgumentException(String.Format("Cannot cast an object of type '{0}' to type '{1}'.", this, destType));

			var otherType = (UserType) destType;
			return base.CastTo(value, destType);
		}
		*/

		public override bool IsIndexable {
			get { return false; }
		}

		public int MemberCount {
			get { return TypeInfo.MemberCount; }
		}

		public SqlUserObject NewObject(params SqlExpression[] args) {
			return NewObject(null, args);
		}

		internal override int ColumnSizeOf(ISqlObject obj) {
			var userObj = (SqlUserObject) obj;

			var size = 1;

			for (int i = 0; i < MemberCount; i++) {
				var member = TypeInfo.MemberAt(i);
				var memberValue = userObj.GetValue(member.MemberName);
				size += member.MemberType.ColumnSizeOf(memberValue);
			}

			return size;
		}

		public SqlUserObject NewObject(IRequest context, SqlExpression[] args = null) {
			var memberCount = TypeInfo.MemberCount;
			var argc = 0;

			if (memberCount > 0) {
				if (args == null || args.Length != memberCount)
					throw new ArgumentException(String.Format("Invalid number of arguments provided to construct an object of type '{0}'.", FullName));

				argc = args.Length;
			}

			var objArgs = new ISqlObject[argc];
			var argNames = new string[memberCount];

			for (int i = 0; i < memberCount; i++) {
				argNames[i] = TypeInfo.MemberAt(i).MemberName;
			}

			if (args != null) {
				for (int i = 0; i < argc; i++) {
					var member = TypeInfo.MemberAt(i);
					var arg = args[0];

					if (!arg.IsConstant())
						throw new InvalidOperationException("Cannot instantiate an object with a non-constant argument");

					Field field;
					if (context != null) {
						field = arg.EvaluateToConstant(context, context.Context.VariableResolver());
					} else {
						field = arg.EvaluateToConstant(null, null);
					}

					if (!field.Type.Equals(member.MemberType) &&
					    !field.Type.CanCastTo(member.MemberType)) {
						throw new InvalidOperationException(
							String.Format("The input argument is not compatible with the type '{0}' of member '{1}'.",
								member.MemberType, member.MemberName));
					}

					objArgs[i] = field.CastTo(member.MemberType).Value;
				}
			}

			var values = new Dictionary<string, ISqlObject>();
			for (int i = 0; i < memberCount; i++) {
				values[argNames[i]] = objArgs[i];
			}

			return new SqlUserObject(values);
		}

		public override void SerializeObject(Stream stream, ISqlObject obj) {
			var writer = new BinaryWriter(stream);
			if (obj == null || obj.IsNull) {
				writer.Write((byte) 0);
			} else {
				writer.Write((byte)1);

				var userObj = (SqlUserObject) obj;
				for (int i = 0; i < MemberCount; i++) {
					var member = TypeInfo.MemberAt(i);
					var memberType = member.MemberType;
					var memberValue = userObj.GetValue(member.MemberName);
					memberType.SerializeObject(stream, memberValue);
				}
			}
		}

		public override ISqlObject DeserializeObject(Stream stream) {
			var reader = new BinaryReader(stream);
			var isNull = reader.ReadByte() == 0;
			if (isNull)
				return SqlUserObject.Null;

			var values = new Dictionary<string, ISqlObject>();

			for (int i = 0; i < MemberCount; i++) {
				var member = TypeInfo.MemberAt(i);
				var memberName = member.MemberName;
				var memberType = member.MemberType;

				var value = memberType.DeserializeObject(stream);
				values[memberName] = value;
			}

			return new SqlUserObject(values);
		}
	}
}
