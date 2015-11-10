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

		private string[] QueryUserGroups(string userName) {
			var table = QueryContext.GetTable(SystemSchema.UserGroupTableName);
			var c1 = table.GetResolvedColumnName(0);
			// All 'user_group' where UserName = %username%
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

		private void ClearUserGroupsCache(string userName) {
			if (userGroupsCache == null)
				return;

			userGroupsCache.Remove(userName);
		}

		private void ClearUserGroupsCache() {
			if (userGroupsCache == null)
				return;

			userGroupsCache.Clear();
		}

		private void SetUserGroupsInCache(string userName, string[] groups) {
			if (userGroupsCache == null)
				userGroupsCache = new Dictionary<string, string[]>();

			userGroupsCache[userName] = groups;
		}

		//public void RevokeAllGrantsOn(DbObjectType objectType, ObjectName objectName) {
		//	var grantTable = QueryContext.GetMutableTable(SystemSchema.UserGrantsTableName);

		//	var objectTypeColumn = grantTable.GetResolvedColumnName(1);
		//	var objectNameColumn = grantTable.GetResolvedColumnName(2);
		//	// All that match the given object
		//	var t1 = grantTable.SimpleSelect(QueryContext, objectTypeColumn, SqlExpressionType.Equal,
		//		SqlExpression.Constant(DataObject.Integer((int)objectType)));
		//	// All that match the given parameter
		//	t1 = t1.SimpleSelect(QueryContext, objectNameColumn, SqlExpressionType.Equal,
		//		SqlExpression.Constant(DataObject.String(objectName.FullName)));

		//	// Remove these rows from the table
		//	grantTable.Delete(t1);
		//}


		public bool DropUser(string userName) {
			var userExpr = SqlExpression.Constant(DataObject.String(userName));

			RemoveUserFromAllGroups(userName);

			// TODO: Remove all object-level privileges from the user...

			//var table = QueryContext.GetMutableTable(SystemSchema.UserConnectPrivilegesTableName);
			//var c1 = table.GetResolvedColumnName(0);
			//var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, userExpr);
			//table.Delete(t);

			var table = QueryContext.GetMutableTable(SystemSchema.PasswordTableName);
			var c1 = table.GetResolvedColumnName(0);
			var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, userExpr);
			table.Delete(t);

			table = QueryContext.GetMutableTable(SystemSchema.UserTableName);
			c1 = table.GetResolvedColumnName(0);
			t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, userExpr);
			return table.Delete(t) > 0;
		}

		private void RemoveUserFromAllGroups(string username) {
			var userExpr = SqlExpression.Constant(DataObject.String(username));

			var table = QueryContext.GetMutableTable(SystemSchema.UserGroupTableName);
			var c1 = table.GetResolvedColumnName(0);
			var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, userExpr);

			if (t.RowCount > 0) {
				table.Delete(t);

				ClearUserGroupsCache(username);
			}
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
			var table = QueryContext.GetMutableTable(SystemSchema.UserGroupTableName);
			var c1 = table.GetResolvedColumnName(0);
			var c2 = table.GetResolvedColumnName(1);
			// All 'user_group' where UserName = %username%
			var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, SqlExpression.Constant(userName));
			// All from this set where PrivGroupName = %group%
			t = t.SimpleSelect(QueryContext, c2, SqlExpressionType.Equal, SqlExpression.Constant(SystemGroups.LockGroup));

			bool userBelongsToLockGroup = t.RowCount > 0;
			if (status == UserStatus.Locked &&
				!userBelongsToLockGroup) {
				// Lock the user by adding the user to the Lock group
				// Add this user to the locked group.
				var rdat = new Row(table);
				rdat.SetValue(0, userName);
				rdat.SetValue(1, SystemGroups.LockGroup);
				table.AddRow(rdat);
			} else if (status == UserStatus.Unlocked &&
				userBelongsToLockGroup) {
				// Unlock the user by removing the user from the Lock group
				// Remove this user from the locked group.
				table.Delete(t);
			}
		}

		public UserStatus GetUserStatus(string userName) {
			if (IsUserInGroup(userName, SystemGroups.LockGroup))
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


		public void CreateUserGroup(string groupName) {
			if (String.IsNullOrEmpty(groupName))
				throw new ArgumentNullException("groupName");

			var c = groupName[0];
			if (c == '$' || c == '%' || c == '@')
				throw new ArgumentException(String.Format("Group name '{0}' starts with an invalid character.", groupName));

			var table = QueryContext.GetMutableTable(SystemSchema.GroupsTableName);

			var row = table.NewRow();
			row.SetValue(0, groupName);

			table.AddRow(row);
		}

		public bool DropUserGroup(string groupName) {
			var table = QueryContext.GetMutableTable(SystemSchema.UserGroupTableName);
			var c1 = table.GetResolvedColumnName(0);

			// All password where name = %groupName%
			var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, SqlExpression.Constant(groupName));

			if (t.RowCount > 0) {
				table.Delete(t);
				ClearUserGroupsCache();
				return true;
			}

			return false;
		}

		public bool UserGroupExists(string groupName) {
			var table = QueryContext.GetTable(SystemSchema.UserGroupTableName);
			var c1 = table.GetResolvedColumnName(0);

			// All password where name = %groupName%
			var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, SqlExpression.Constant(groupName));
			return t.RowCount > 0;
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
				var table = QueryContext.GetMutableTable(SystemSchema.UserGroupTableName);
				var row = table.NewRow();
				row.SetValue(0, userName);
				row.SetValue(1, groupName);
				table.AddRow(row);
			}
		}

		public bool RemoveUserFromGroup(string userName, string groupName) {
			// This is a special query that needs to access the lowest level of ITable, skipping
			// other security controls
			var table = QueryContext.GetMutableTable(SystemSchema.UserGroupTableName);
			var c1 = table.GetResolvedColumnName(0);
			var c2 = table.GetResolvedColumnName(1);

			// All 'user_group' where UserName = %username%
			var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, SqlExpression.Constant(userName));

			// All from this set where PrivGroupName = %group%
			t = t.SimpleSelect(QueryContext, c2, SqlExpressionType.Equal, SqlExpression.Constant(groupName));

			if (t.RowCount > 0) {
				table.Delete(t);

				ClearUserGroupsCache(userName);

				return true;
			}

			return false;
		}

		public bool IsUserInGroup(string userName, string groupName) {
			string[] userGroups;
			if (TryGetUserGroupsFromCache(userName, out userGroups) &&
			    userGroups.Any(x => String.Equals(groupName, x, StringComparison.OrdinalIgnoreCase)))
				return true;

			// This is a special query that needs to access the lowest level of ITable, skipping
			// other security controls
			var table = QueryContext.GetTable(SystemSchema.UserGroupTableName);
			var c1 = table.GetResolvedColumnName(0);
			var c2 = table.GetResolvedColumnName(1);

			// All 'user_group' where UserName = %username%
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

	}
}
