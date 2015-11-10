using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Security {
	public class PrivilegeManager : IPrivilegeManager {
		private Dictionary<GrantCacheKey, Privileges> grantsCache;
		private Dictionary<string, Privileges> groupsPrivilegesCache;

		public PrivilegeManager(IQueryContext queryContext) {
			QueryContext = queryContext;
		}

		~PrivilegeManager() {
			Dispose(false);
		}

		public IQueryContext QueryContext { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			QueryContext = null;
		}

		private void UpdateUserGrants(DbObjectType objectType, ObjectName objectName, string granter, string user, Privileges privileges, bool withOption) {
			// Revoke existing privs on this object for this grantee
			RevokeAllGrantsFromUser(objectType, objectName, granter, user, withOption);

			if (privileges != Privileges.None) {
				// The system grants table.
				var grantTable = QueryContext.GetMutableTable(SystemSchema.UserGrantsTableName);

				// Add the grant to the grants table.
				var row = grantTable.NewRow();
				row.SetValue(0, (int)privileges);
				row.SetValue(1, (int)objectType);
				row.SetValue(2, objectName.FullName);
				row.SetValue(3, user);
				row.SetValue(4, withOption);
				row.SetValue(5, granter);
				grantTable.AddRow(row);

				ClearUserGrantsCache(user, objectType, objectName, withOption, true);
			}
		}

		private void ClearUserGrantsCache(string userName, DbObjectType objectType, ObjectName objectName, bool withOption,
			bool withPublic) {
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

		public void GrantToUser(string userName, UserGrant grant) {
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
				SqlExpression.Constant(DataObject.String(objectName.FullName)));

			// The next is a single exhaustive select through the remaining records.
			// It finds all grants that match either public or the grantee is the
			// username, and that match the object type.

			// Expression: ("grantee_col" = username)
			var userCheck = SqlExpression.Equal(SqlExpression.Reference(granteeCol),
				SqlExpression.Constant(DataObject.String(user)));

			// Expression: ("object_col" = object AND
			//              "grantee_col" = username)
			// All that match the given username or public and given object
			var expr =
				SqlExpression.And(
					SqlExpression.Equal(SqlExpression.Reference(objectCol),
						SqlExpression.Constant(DataObject.BigInt((int)objectType))), userCheck);

			// Are we only searching for grant options?
			var grantOptionCheck = SqlExpression.Equal(SqlExpression.Reference(grantOptionCol),
				SqlExpression.Constant(DataObject.Boolean(withOption)));
			expr = SqlExpression.And(expr, grantOptionCheck);

			// Make sure the granter matches up also
			var granterCheck = SqlExpression.Equal(SqlExpression.Reference(granterCol),
				SqlExpression.Constant(DataObject.String(revoker)));
			expr = SqlExpression.And(expr, granterCheck);

			t1 = t1.ExhaustiveSelect(QueryContext, expr);

			// Remove these rows from the table
			grantTable.Delete(t1);
		}


		private Privileges QueryUserPrivileges(string userName, DbObjectType objectType, ObjectName objectName,
			bool withOption, bool withPublic) {
			// The system grants table.
			var grantTable = QueryContext.GetTable(SystemSchema.UserGrantsTableName);

			var objectCol = grantTable.GetResolvedColumnName(1);
			var paramCol = grantTable.GetResolvedColumnName(2);
			var granteeCol = grantTable.GetResolvedColumnName(3);
			var grantOptionCol = grantTable.GetResolvedColumnName(4);
			var granterCol = grantTable.GetResolvedColumnName(5);

			ITable t1 = grantTable;

			// All that match the given object parameter
			// It's most likely this will reduce the search by the most so we do
			// it first.
			t1 = t1.SimpleSelect(QueryContext, paramCol, SqlExpressionType.Equal, SqlExpression.Constant(DataObject.String(objectName.FullName)));

			// The next is a single exhaustive select through the remaining records.
			// It finds all grants that match either public or the grantee is the
			// username, and that match the object type.

			// Expression: ("grantee_col" = username OR "grantee_col" = 'public')
			var userCheck = SqlExpression.Equal(SqlExpression.Reference(granteeCol), SqlExpression.Constant(DataObject.String(userName)));
			if (withPublic) {
				userCheck = SqlExpression.Or(userCheck, SqlExpression.Equal(SqlExpression.Reference(granteeCol),
					SqlExpression.Constant(DataObject.String(User.PublicName))));
			}

			// Expression: ("object_col" = object AND
			//              ("grantee_col" = username OR "grantee_col" = 'public'))
			// All that match the given username or public and given object
			var expr = SqlExpression.And(SqlExpression.Equal(SqlExpression.Reference(objectCol),
				SqlExpression.Constant(DataObject.BigInt((int)objectType))), userCheck);

			// Are we only searching for grant options?
			if (withOption) {
				var grantOptionCheck = SqlExpression.Equal(SqlExpression.Reference(grantOptionCol),
					SqlExpression.Constant(DataObject.BooleanTrue));
				expr = SqlExpression.And(expr, grantOptionCheck);
			}

			t1 = t1.ExhaustiveSelect(QueryContext, expr);

			// For each grant, merge with the resultant priv object
			Privileges privs = Privileges.None;

			foreach (var row in t1) {
				var priv = (int)row.GetValue(0).AsBigInt();
				privs |= (Privileges)priv;
			}

			return privs;
		}

		public Privileges GetUserPrivileges(string userName, DbObjectType objectType, ObjectName objectName, bool withOption) {
			Privileges privs;
			if (!TryGetPrivilegesFromCache(userName, objectType, objectName, withOption, true, out privs)) {
				privs = QueryUserPrivileges(userName, objectType, objectName, withOption, true);
				SetPrivilegesInCache(userName, objectType, objectName, withOption, true, privs);
			}

			return privs;
		}

		public void RevokeFromUser(string userName, UserGrant grant) {
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			try {
				RevokeAllGrantsFromUser(grant.ObjectType, grant.ObjectName, grant.GranterName, userName, grant.WithOption);
			} finally {
				ClearUserGrantsCache(userName, grant.ObjectType, grant.ObjectName, grant.WithOption, false);
			}
		}

		public void GrantToGroup(string groupName, Privileges privileges) {
			throw new NotImplementedException();
		}

		public void RevokeFromGroup(string groupName, Privileges privileges) {
			throw new NotImplementedException();
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
