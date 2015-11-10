using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;

namespace Deveel.Data.Security {
	public class UserManager : IUserManager {
		private Dictionary<string, string[]> userGroupsCache;
		private Dictionary<GrantCacheKey, Privileges> grantsCache;
		private Dictionary<string, Privileges> groupsPrivilegesCache; 
		 
		public UserManager(IQueryContext queryContext) {
			QueryContext = queryContext;
		}

		~UserManager() {
			Dispose(false);
		}

		public IQueryContext QueryContext { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			userGroupsCache = null;
			grantsCache = null;
			groupsPrivilegesCache = null;
			QueryContext = null;
		}

		public bool UserExists(string userName) {
			var table = QueryContext.GetTable(SystemSchema.UserTableName);
			var c1 = table.GetResolvedColumnName(0);

			// All password where UserName = %username%
			var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, SqlExpression.Constant(userName));
			return t.RowCount > 0;
		}

		public void CreateUser(UserInfo userInfo, string identifier) {
			if (userInfo == null)
				throw new ArgumentNullException("userInfo");
			if (String.IsNullOrEmpty(identifier))
				throw new ArgumentNullException("identifier");

			// TODO: make these rules configurable?

			var userName = userInfo.Name;

			if (userName.Length <= 1)
				throw new ArgumentException("User name must be at least one character.");
			if (identifier.Length <= 1)
				throw new ArgumentException("The password must be at least one character.");

			if (String.Equals(userName, User.PublicName, StringComparison.OrdinalIgnoreCase))
				throw new ArgumentException(String.Format("User name '{0}' is reserved and cannot be registered.", User.PublicName), "userName");

			var c = userName[0];
			if (c == '#' || c == '@' || c == '$' || c == '&')
				throw new ArgumentException(String.Format("User name '{0}' is invalid: cannot start with '{1}' character.", userName, c), "userName");
			if (UserExists(userName))
				throw new SecurityException(String.Format("User '{0}' is already registered.", userName));

			// Add to the key 'user' table
			var table = QueryContext.GetMutableTable(SystemSchema.UserTableName);
			var row = table.NewRow();
			row[0] = DataObject.String(userName);
			table.AddRow(row);

			var method = userInfo.Identification.Method;
			var methodArgs = SerializeArguments(userInfo.Identification.Arguments);

			if (method != "plain")
				throw new NotImplementedException("Only mechanism implemented right now is plain text (it sucks!)");

			table = QueryContext.GetMutableTable(SystemSchema.PasswordTableName);
			row = table.NewRow();
			row.SetValue(0, userName);
			row.SetValue(1, method);
			row.SetValue(2, methodArgs);
			row.SetValue(3, identifier);
			table.AddRow(row);
		}

		private static byte[] SerializeArguments(IDictionary<string, object> args) {
			using (var stream = new MemoryStream()) {
				using (var writer = new BinaryWriter(stream)) {
					writer.Write(args.Count);

					foreach (var arg in args) {
						writer.Write(arg.Key);

						if (arg.Value is bool) {
							writer.Write((byte)1);
							writer.Write((bool) arg.Value);
						} else if (arg.Value is short ||
						           arg.Value is int ||
						           arg.Value is long) {
							var value = (long) arg.Value;
							writer.Write((byte)2);
							writer.Write(value);
						} else if (arg.Value is string) {
							writer.Write((byte)3);
							writer.Write((string) arg.Value);
						}
					}

					writer.Flush();
					return stream.ToArray();
				}
			}
		}

		private static IDictionary<string, object> DeserializeArguments(byte[] bytes) {
			using (var stream = new MemoryStream(bytes)) {
				using (var reader = new BinaryReader(stream)) {
					var argCount = reader.ReadInt32();

					var args = new Dictionary<string, object>(argCount);
					for (int i = 0; i < argCount; i++) {
						var argName = reader.ReadString();
						var argType = reader.ReadByte();
						object value = null;
						if (argType == 1) {
							value = reader.ReadBoolean();
						} else if (argType == 2) {
							value = reader.ReadInt64();
						} else if (argType == 3) {
							value = reader.ReadString();
						}

						args[argName] = value;
					}

					return args;
				}
			}
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

		private string[] QueryUserGroups(string userName) {
			var table = QueryContext.GetTable(SystemSchema.UserPrivilegesTableName);
			var c1 = table.GetResolvedColumnName(0);
			// All 'user_priv' where UserName = %username%
			var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, SqlExpression.Constant(DataObject.String(userName)));
			int sz = t.RowCount;
			var groups = new string[sz];
			var rowEnum = t.GetEnumerator();
			int i = 0;
			while (rowEnum.MoveNext()) {
				groups[i] = t.GetValue(rowEnum.Current.RowId.RowNumber, 1).Value.ToString();
				++i;
			}

			return groups;
		}

		private bool TryGetUserGroupsFromCache(string userName, out string[] groups) {
			if (userGroupsCache == null) {
				groups = null;
				return false;
			}

			return userGroupsCache.TryGetValue(userName, out groups);
		}

		private void SetUserGroupsInCache(string userName, string[] groups) {
			if (userGroupsCache == null)
				userGroupsCache = new Dictionary<string, string[]>();

			userGroupsCache[userName] = groups;
		}

		public void RevokeAllGrantsOn(DbObjectType objectType, ObjectName objectName) {
			var grantTable = QueryContext.GetMutableTable(SystemSchema.UserGrantsTableName);

			var objectTypeColumn = grantTable.GetResolvedColumnName(1);
			var objectNameColumn = grantTable.GetResolvedColumnName(2);
			// All that match the given object
			var t1 = grantTable.SimpleSelect(QueryContext, objectTypeColumn, SqlExpressionType.Equal,
				SqlExpression.Constant(DataObject.Integer((int)objectType)));
			// All that match the given parameter
			t1 = t1.SimpleSelect(QueryContext, objectNameColumn, SqlExpressionType.Equal,
				SqlExpression.Constant(DataObject.String(objectName.FullName)));

			// Remove these rows from the table
			grantTable.Delete(t1);
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


		public bool DropUser(string userName) {
			var userExpr = SqlExpression.Constant(DataObject.String(userName));

			RemoveUserFromAllGroups(userName);
			ClearUserGrantsCache(userName);

			// TODO: Remove all object-level privileges from the user...

			var table = QueryContext.GetMutableTable(SystemSchema.UserConnectPrivilegesTableName);
			var c1 = table.GetResolvedColumnName(0);
			var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, userExpr);
			table.Delete(t);

			table = QueryContext.GetMutableTable(SystemSchema.PasswordTableName);
			c1 = table.GetResolvedColumnName(0);
			t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, userExpr);
			table.Delete(t);

			table = QueryContext.GetMutableTable(SystemSchema.UserTableName);
			c1 = table.GetResolvedColumnName(0);
			t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, userExpr);
			return table.Delete(t) > 0;
		}

		private void RemoveUserFromAllGroups(string username) {
			var userExpr = SqlExpression.Constant(DataObject.String(username));

			var table = QueryContext.GetMutableTable(SystemSchema.UserPrivilegesTableName);
			var c1 = table.GetResolvedColumnName(0);
			var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, userExpr);
			table.Delete(t);
		}

		public void AlterUser(UserInfo userInfo, string identifier) {
			var userName = userInfo.Name;

			var userExpr = SqlExpression.Constant(DataObject.String(userName));

			// Delete the current username from the 'password' table
			var table = QueryContext.GetMutableTable(SystemSchema.PasswordTableName);
			var c1 = table.GetResolvedColumnName(0);
			var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, userExpr);
			if (t.RowCount != 1)
				throw new SecurityException(String.Format("User '{0}' was not found.", userName));

			table.Delete(t);

			// TODO: get the hash algorithm and hash ...

			var method = userInfo.Identification.Method;
			var methodArgs = SerializeArguments(userInfo.Identification.Arguments);

			if (method != "plain")
				throw new NotImplementedException("Only mechanism implemented right now is plain text (it sucks!)");

			// Add the new username
			table = QueryContext.GetMutableTable(SystemSchema.PasswordTableName);
			var row = table.NewRow();
			row.SetValue(0, userName);
			row.SetValue(1, method);
			row.SetValue(2, methodArgs);
			row.SetValue(3, identifier);
			table.AddRow(row);

		}

		public void SetUserStatus(string userName, UserStatus status) {
			// Internally we implement this by adding the user to the #locked group.
			var table = QueryContext.GetMutableTable(SystemSchema.UserPrivilegesTableName);
			var c1 = table.GetResolvedColumnName(0);
			var c2 = table.GetResolvedColumnName(1);
			// All 'user_priv' where UserName = %username%
			var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, SqlExpression.Constant(userName));
			// All from this set where PrivGroupName = %group%
			t = t.SimpleSelect(QueryContext, c2, SqlExpressionType.Equal, SqlExpression.Constant(SystemGroupNames.LockGroup));

			bool userBelongsToLockGroup = t.RowCount > 0;
			if (status == UserStatus.Locked &&
				!userBelongsToLockGroup) {
				// Lock the user by adding the user to the Lock group
				// Add this user to the locked group.
				var rdat = new Row(table);
				rdat.SetValue(0, userName);
				rdat.SetValue(1, SystemGroupNames.LockGroup);
				table.AddRow(rdat);
			} else if (status == UserStatus.Unlocked &&
				userBelongsToLockGroup) {
				// Unlock the user by removing the user from the Lock group
				// Remove this user from the locked group.
				table.Delete(t);
			}
		}

		public UserStatus GetUserStatus(string userName) {
			if (IsUserInGroup(userName, SystemGroupNames.LockGroup))
				return UserStatus.Locked;

			return UserStatus.Unlocked;
		}

		public UserInfo GetUser(string userName) {
			var table = QueryContext.GetTable(SystemSchema.PasswordTableName);
			var unameColumn = table.GetResolvedColumnName(0);
			var methodColumn = table.GetResolvedColumnName(1);
			var methodArgsColumn = table.GetResolvedColumnName(2);

			var t = table.SimpleSelect(QueryContext, unameColumn, SqlExpressionType.Equal, SqlExpression.Constant(userName));
			if (t.RowCount == 0)
				throw new SecurityException(String.Format("User '{0}' is not registered.", userName));

			var method = t.GetValue(0, methodColumn);
			var methodArgs = t.GetValue(0, methodArgsColumn);
			var argBytes = ((SqlBinary) methodArgs.Value).ToByteArray();
			var args = DeserializeArguments(argBytes);

			var identification = new UserIdentification(method);
			foreach (var arg in args) {
				identification.Arguments[arg.Key] = arg.Value;
			}

			return new UserInfo(userName, identification);
		}

		public bool CheckIdentifier(string userName, string identifier) {
			var table = QueryContext.GetTable(SystemSchema.PasswordTableName);
			var unameColumn = table.GetResolvedColumnName(0);
			var idColumn = table.GetResolvedColumnName(3);

			var t = table.SimpleSelect(QueryContext, unameColumn, SqlExpressionType.Equal, SqlExpression.Constant(userName));
			if (t.RowCount == 0)
				throw new SecurityException(String.Format("User '{0}' is not registered.", userName));

			var stored = t.GetValue(0, idColumn);
			return stored.Value.ToString().Equals(identifier);
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
			throw new NotImplementedException();
		}

		public void CreateUserGroup(string groupName) {
			throw new NotImplementedException();
		}

		public bool DropUserGroup(string groupName) {
			throw new NotImplementedException();
		}

		public void GrantToGroup(string groupName, Privileges privileges) {
			throw new NotImplementedException();
		}

		public void RevokeFromGroup(string groupName, Privileges privileges) {
			throw new NotImplementedException();
		}

		public void AddUserToGroup(string userName, string groupName) {
			if (String.IsNullOrEmpty(groupName))
				throw new ArgumentNullException("group");
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("username");

			char c = groupName[0];
			if (c == '@' || c == '&' || c == '#' || c == '$')
				throw new ArgumentException(String.Format("Group name '{0}' is invalid: cannot start with {1}", groupName, c), "groupName");

			if (!IsUserInGroup(userName, groupName)) {
				var table = QueryContext.GetMutableTable(SystemSchema.UserPrivilegesTableName);
				var row = table.NewRow();
				row.SetValue(0, userName);
				row.SetValue(1, groupName);
				table.AddRow(row);
			}
		}

		public bool IsUserInGroup(string userName, string groupName) {
			string[] userGroups;
			if (TryGetUserGroupsFromCache(userName, out userGroups) &&
			    userGroups.Any(x => String.Equals(groupName, x, StringComparison.OrdinalIgnoreCase)))
				return true;

			// This is a special query that needs to access the lowest level of ITable, skipping
			// other security controls
			var table = QueryContext.GetTable(SystemSchema.UserPrivilegesTableName);
			var c1 = table.GetResolvedColumnName(0);
			var c2 = table.GetResolvedColumnName(1);

			// All 'user_priv' where UserName = %username%
			var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, SqlExpression.Constant(userName));

			// All from this set where PrivGroupName = %group%
			t = t.SimpleSelect(QueryContext, c2, SqlExpressionType.Equal, SqlExpression.Constant(groupName));
			return t.RowCount > 0;
		}

		public string[] GetUserGroups(string userName) {
			string[] groups;
			if (!TryGetUserGroupsFromCache(userName, out groups)) {
				groups = QueryUserGroups(userName);
				SetUserGroupsInCache(userName, groups);
			}

			return groups;
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
				return unchecked (((userName.GetHashCode()*objectName.GetHashCode()) ^ (int) objectType) + options);
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
