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
using System.Linq;

using Deveel.Data.Services;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Security {
	public class PrivilegeManager : IPrivilegeManager/*, IResolveCallback*/ {
		private Dictionary<GrantCacheKey, Privileges> grantsCache;
		private Dictionary<string, Privileges> groupsPrivilegesCache;

		public PrivilegeManager(IQuery queryContext) {
			QueryContext = queryContext;
		}

		~PrivilegeManager() {
			Dispose(false);
		}

		public IQuery QueryContext { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		//void IResolveCallback.OnResolved(IResolveScope scope) {
		//	var context = scope as IQueryContext;
		//	if (context == null)
		//		throw new InvalidOperationException("Privilege manager resolved outside the scope of the query context.");

		//	QueryContext = context;
		//}

		protected virtual void Dispose(bool disposing) {
			QueryContext = null;
		}

		private static void UpdateGrants(IQuery queryContext, IMutableTable grantTable, DbObjectType objectType, ObjectName objectName,
			string granter, string grantee, Privileges privileges, bool withOption) {
			RevokeAllGrants(queryContext, grantTable, objectType, objectName, granter, grantee, withOption);

			if (privileges != Privileges.None) {
				// Add the grant to the grants table.
				var row = grantTable.NewRow();
				row.SetValue(0, (int)privileges);
				row.SetValue(1, (int)objectType);
				row.SetValue(2, objectName.FullName);
				row.SetValue(3, grantee);
				row.SetValue(4, withOption);
				row.SetValue(5, granter);
				grantTable.AddRow(row);
			}
		}

		private static void RevokeAllGrants(IQuery queryContext, IMutableTable grantTable, DbObjectType objectType, ObjectName objectName, string revoker, string user, bool withOption = false) {
			var objectCol = grantTable.GetResolvedColumnName(1);
			var paramCol = grantTable.GetResolvedColumnName(2);
			var granteeCol = grantTable.GetResolvedColumnName(3);
			var grantOptionCol = grantTable.GetResolvedColumnName(4);
			var granterCol = grantTable.GetResolvedColumnName(5);

			ITable t1 = grantTable;

			// All that match the given object parameter
			// It's most likely this will reduce the search by the most so we do
			// it first.
			t1 = t1.SimpleSelect(queryContext, paramCol, SqlExpressionType.Equal,
				SqlExpression.Constant(Field.String(objectName.FullName)));

			// The next is a single exhaustive select through the remaining records.
			// It finds all grants that match either public or the grantee is the
			// username, and that match the object type.

			// Expression: ("grantee_col" = username)
			var userCheck = SqlExpression.Equal(SqlExpression.Reference(granteeCol),
				SqlExpression.Constant(Field.String(user)));

			// Expression: ("object_col" = object AND
			//              "grantee_col" = username)
			// All that match the given username or public and given object
			var expr =
				SqlExpression.And(
					SqlExpression.Equal(SqlExpression.Reference(objectCol),
						SqlExpression.Constant(Field.BigInt((int)objectType))), userCheck);

			// Are we only searching for grant options?
			var grantOptionCheck = SqlExpression.Equal(SqlExpression.Reference(grantOptionCol),
				SqlExpression.Constant(Field.Boolean(withOption)));
			expr = SqlExpression.And(expr, grantOptionCheck);

			// Make sure the granter matches up also
			var granterCheck = SqlExpression.Equal(SqlExpression.Reference(granterCol),
				SqlExpression.Constant(Field.String(revoker)));
			expr = SqlExpression.And(expr, granterCheck);

			t1 = t1.ExhaustiveSelect(queryContext, expr);

			// Remove these rows from the table
			grantTable.Delete(t1);
		}


		private void UpdateUserGrants(DbObjectType objectType, ObjectName objectName, string granter, string grantee, Privileges privileges, bool withOption) {
			var grantTable = QueryContext.GetMutableTable(SystemSchema.UserGrantsTableName);

			try {
				UpdateGrants(QueryContext, grantTable, objectType, objectName, granter, grantee, privileges, withOption);
			} finally {
				ClearUserGrantsCache(grantee, objectType, objectName, withOption, true);
			}
		}

		private void ClearUserGrantsCache(string userName, DbObjectType objectType, ObjectName objectName, bool withOption, bool withPublic) {
			if (grantsCache == null)
				return;

			var key = new GrantCacheKey(userName, objectType, objectName.FullName, withOption, withPublic);
			grantsCache.Remove(key);
		}

		private void ClearUserGrantsCache(string userName) {
			if (grantsCache == null)
				return;

			var keys = grantsCache.Keys.Where(x => x.userName.Equals(userName, StringComparison.OrdinalIgnoreCase));
			foreach (var key in keys) {
				grantsCache.Remove(key);
			}
		}

		public void GrantToUser(string userName, Grant grant) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");
			if (grant == null)
				throw new ArgumentNullException("grant");

			var objectType = grant.ObjectType;
			var objectName = grant.ObjectName;
			var privileges = grant.Privileges;

			Privileges oldPrivs = GetUserPrivileges(userName, objectType, objectName, grant.WithOption);
			privileges |= oldPrivs;

			if (!oldPrivs.Equals(privileges))
				UpdateUserGrants(objectType, objectName, grant.GranterName, userName, privileges, grant.WithOption);
		}

		private bool TryGetPrivilegesFromCache(string userName, DbObjectType objectType, ObjectName objectName, bool withOption, bool withPublic,
			out Privileges privileges) {
			if (grantsCache == null) {
				privileges = Privileges.None;
				return false;
			}

			var key = new GrantCacheKey(userName, objectType, objectName.FullName, withOption, withPublic);
			return grantsCache.TryGetValue(key, out privileges);
		}

		private void SetPrivilegesInCache(string userName, DbObjectType objectType, ObjectName objectName, bool withOption, bool withPublic,
			Privileges privileges) {
			var key = new GrantCacheKey(userName, objectType, objectName.FullName, withOption, withPublic);
			if (grantsCache == null)
				grantsCache = new Dictionary<GrantCacheKey, Privileges>();

			grantsCache[key] = privileges;
		}

		private void RevokeAllGrantsFromUser(DbObjectType objectType, ObjectName objectName, string revoker, string user, bool withOption = false) {
			var grantTable = QueryContext.GetMutableTable(SystemSchema.UserGrantsTableName);

			var objectCol = grantTable.GetResolvedColumnName(1);
			var paramCol = grantTable.GetResolvedColumnName(2);
			var granteeCol = grantTable.GetResolvedColumnName(3);
			var grantOptionCol = grantTable.GetResolvedColumnName(4);
			var granterCol = grantTable.GetResolvedColumnName(5);

			ITable t1 = grantTable;

			// All that match the given object parameter
			// It's most likely this will reduce the search by the most so we do
			// it first.
			t1 = t1.SimpleSelect(QueryContext, paramCol, SqlExpressionType.Equal,
				SqlExpression.Constant(Field.String(objectName.FullName)));

			// The next is a single exhaustive select through the remaining records.
			// It finds all grants that match either public or the grantee is the
			// username, and that match the object type.

			// Expression: ("grantee_col" = username)
			var userCheck = SqlExpression.Equal(SqlExpression.Reference(granteeCol),
				SqlExpression.Constant(Field.String(user)));

			// Expression: ("object_col" = object AND
			//              "grantee_col" = username)
			// All that match the given username or public and given object
			var expr =
				SqlExpression.And(
					SqlExpression.Equal(SqlExpression.Reference(objectCol),
						SqlExpression.Constant(Field.BigInt((int)objectType))), userCheck);

			// Are we only searching for grant options?
			var grantOptionCheck = SqlExpression.Equal(SqlExpression.Reference(grantOptionCol),
				SqlExpression.Constant(Field.Boolean(withOption)));
			expr = SqlExpression.And(expr, grantOptionCheck);

			// Make sure the granter matches up also
			var granterCheck = SqlExpression.Equal(SqlExpression.Reference(granterCol),
				SqlExpression.Constant(Field.String(revoker)));
			expr = SqlExpression.And(expr, granterCheck);

			t1 = t1.ExhaustiveSelect(QueryContext, expr);

			// Remove these rows from the table
			grantTable.Delete(t1);
		}

		private static Privileges QueryPrivileges(IQuery queryContext, ITable grantTable, string grantee,
			DbObjectType objectType, ObjectName objectName, bool withOption, bool withPublic) {
			var objectCol = grantTable.GetResolvedColumnName(1);
			var paramCol = grantTable.GetResolvedColumnName(2);
			var granteeCol = grantTable.GetResolvedColumnName(3);
			var grantOptionCol = grantTable.GetResolvedColumnName(4);
			var granterCol = grantTable.GetResolvedColumnName(5);

			ITable t1 = grantTable;

			// All that match the given object parameter
			// It's most likely this will reduce the search by the most so we do
			// it first.
			t1 = t1.SimpleSelect(queryContext, paramCol, SqlExpressionType.Equal, SqlExpression.Constant(Field.String(objectName.FullName)));

			// The next is a single exhaustive select through the remaining records.
			// It finds all grants that match either public or the grantee is the
			// username, and that match the object type.

			// Expression: ("grantee_col" = username OR "grantee_col" = 'public')
			var userCheck = SqlExpression.Equal(SqlExpression.Reference(granteeCol), SqlExpression.Constant(Field.String(grantee)));
			if (withPublic) {
				userCheck = SqlExpression.Or(userCheck, SqlExpression.Equal(SqlExpression.Reference(granteeCol),
					SqlExpression.Constant(Field.String(User.PublicName))));
			}

			// Expression: ("object_col" = object AND
			//              ("grantee_col" = username OR "grantee_col" = 'public'))
			// All that match the given username or public and given object
			var expr = SqlExpression.And(SqlExpression.Equal(SqlExpression.Reference(objectCol),
				SqlExpression.Constant(Field.BigInt((int)objectType))), userCheck);

			// Are we only searching for grant options?
			if (withOption) {
				var grantOptionCheck = SqlExpression.Equal(SqlExpression.Reference(grantOptionCol),
					SqlExpression.Constant(Field.BooleanTrue));
				expr = SqlExpression.And(expr, grantOptionCheck);
			}

			t1 = t1.ExhaustiveSelect(queryContext, expr);

			// For each grant, merge with the resultant priv object
			Privileges privs = Privileges.None;

			foreach (var row in t1) {
				var priv = (int)row.GetValue(0).AsBigInt();
				privs |= (Privileges)priv;
			}

			return privs;
		}

		private Privileges QueryUserPrivileges(string userName, DbObjectType objectType, ObjectName objectName,
			bool withOption, bool withPublic) {
			// The system grants table.
			var grantTable = QueryContext.GetTable(SystemSchema.UserGrantsTableName);
			return QueryPrivileges(QueryContext, grantTable, userName, objectType, objectName, withOption, withPublic);
		}

		private Privileges QueryGroupPrivileges(string groupName, DbObjectType objectType, ObjectName objectName,
			bool withOption, bool withPublic) {
			var grantTable = QueryContext.GetTable(SystemSchema.GroupGrantsTable);
			return QueryPrivileges(QueryContext, grantTable, groupName, objectType, objectName, withOption, withPublic);
		}

		public Privileges GetUserPrivileges(string userName, DbObjectType objectType, ObjectName objectName, bool withOption) {
			Privileges privs;
			if (!TryGetPrivilegesFromCache(userName, objectType, objectName, withOption, true, out privs)) {
				privs = QueryUserPrivileges(userName, objectType, objectName, withOption, true);
				SetPrivilegesInCache(userName, objectType, objectName, withOption, true, privs);
			}

			return privs;
		}

		public void RevokeFromUser(string userName, Grant grant) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			try {
				RevokeAllGrantsFromUser(grant.ObjectType, grant.ObjectName, grant.GranterName, userName, grant.WithOption);
			} finally {
				ClearUserGrantsCache(userName, grant.ObjectType, grant.ObjectName, grant.WithOption, false);
			}
		}

		public void GrantToGroup(string groupName, Grant grant) {
			throw new NotImplementedException();
		}

		public void RevokeFromGroup(string groupName, Grant grant) {
			throw new NotImplementedException();
		}

		public Privileges GetGroupPrivileges(string groupName, DbObjectType objectType, ObjectName objectName) {
			Privileges privileges;
			if (!TryGetPrivilegesFromCache(groupName, objectType, objectName, false, false, out privileges)) {
				privileges = QueryGroupPrivileges(groupName, objectType, objectName, false, false);
				SetPrivilegesInCache(groupName, objectType, objectName, false, false, privileges);
			}

			return privileges;
		}

		#region GrantCacheKey

		class GrantCacheKey : IEquatable<GrantCacheKey> {
			public readonly string userName;
			private readonly DbObjectType objectType;
			private readonly string objectName;
			private readonly int options;

			public GrantCacheKey(string userName, DbObjectType objectType, string objectName, bool withOption, bool withPublic) {
				this.userName = userName;
				this.objectType = objectType;
				this.objectName = objectName;

				options = 0;
				if (withOption)
					options++;
				if (withPublic)
					options++;
			}

			public override bool Equals(object obj) {
				var other = obj as GrantCacheKey;
				return Equals(other);
			}

			public override int GetHashCode() {
				return unchecked(((userName.GetHashCode() * objectName.GetHashCode()) ^ (int)objectType) + options);
			}

			public bool Equals(GrantCacheKey other) {
				if (other == null)
					return false;

				if (!String.Equals(userName, other.userName, StringComparison.OrdinalIgnoreCase))
					return false;

				if (objectType != other.objectType)
					return false;

				if (!String.Equals(objectName, other.objectName, StringComparison.OrdinalIgnoreCase))
					return false;

				if (options != other.options)
					return false;

				return true;
			}
		}

		#endregion
	}
}
