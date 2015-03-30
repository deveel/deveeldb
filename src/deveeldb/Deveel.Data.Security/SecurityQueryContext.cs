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
using System.Linq.Expressions;

using Deveel.Data.DbSystem;
using Deveel.Data.Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Security {
	public static class SecurityQueryContext {
		public static bool UserExists(this IQueryContext context, string userName) {
			var table = context.GetDbTable(SystemSchema.UserTableName);
			var c1 = table.GetResolvedColumnName(0);

			// All password where UserName = %username%
			var t = table.SimpleSelect(context, c1, BinaryOperator.Equal, SqlExpression.Constant(userName));
			return t.RowCount > 0;
		}

		public static Privileges GetUserGrants(this IQueryContext context, DbObjectType objType, ObjectName objName) {
			throw new NotSupportedException();
		}

		public static bool UserHasObjectGrant(this IQueryContext context, ObjectName objectName, DbObjectType objectType, Privilege grant) {
			// The internal secure user has full privs on everything
			if (context.User().IsSystem)
				return true;

			// TODO: Support column level privileges.

			var privs = context.GetUserGrants(objectType, objectName);

			return privs.Permits(grant);
		}
	}
}
