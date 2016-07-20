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

		public override bool CanCastTo(SqlType destType) {
			return false;
		}

		public override bool IsIndexable {
			get { return false; }
		}

		public int MemberCount {
			get { return TypeInfo.MemberCount; }
		}

		public SqlUserObject NewObject(params SqlExpression[] args) {
			return NewObject(null, args);
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
	}
}
