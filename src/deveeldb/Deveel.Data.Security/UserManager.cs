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
using System.IO;
using System.Linq;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Security {
	public class UserManager : IUserManager {		 
		public UserManager(ISession session) {
			if (session == null)
				throw new ArgumentNullException("session");

			Session = session;
		}

		~UserManager() {
			Dispose(false);
		}

		public ISession Session { get; private set; }

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}

		protected virtual void Dispose(bool disposing) {
			Session = null;
		}

		public bool UserExists(string userName) {
			using (var query = Session.CreateQuery()) {
				var table = query.Access.GetTable(SystemSchema.UserTableName);
				var c1 = table.GetResolvedColumnName(0);

				// All password where UserName = %username%
				var t = table.SimpleSelect(query, c1, SqlExpressionType.Equal, SqlExpression.Constant(userName));
				return t.RowCount > 0;
			}
		}

		public void CreateUser(UserInfo userInfo, string identifier) {
			if (userInfo == null)
				throw new ArgumentNullException("userInfo");
			if (String.IsNullOrEmpty(identifier))
				throw new ArgumentNullException("identifier");

			// TODO: make these rules configurable?

			var userName = userInfo.Name;

			if (UserExists(userName))
				throw new SecurityException(String.Format("User '{0}' is already registered.", userName));

			using (var query = Session.CreateQuery()) {
				// Add to the key 'user' table
				var table = query.Access.GetMutableTable(SystemSchema.UserTableName);
				var row = table.NewRow();
				row[0] = Field.String(userName);
				table.AddRow(row);

				var method = userInfo.Identification.Method;
				var methodArgs = SerializeArguments(userInfo.Identification.Arguments);

				if (method != "plain")
					throw new NotImplementedException("Only mechanism implemented right now is plain text (it sucks!)");

				table = query.Access.GetMutableTable(SystemSchema.PasswordTableName);
				row = table.NewRow();
				row.SetValue(0, userName);
				row.SetValue(1, method);
				row.SetValue(2, methodArgs);
				row.SetValue(3, identifier);
				table.AddRow(row);
			}
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

		private string[] QueryUserRoles(string userName) {
			using (var query = Session.CreateQuery()) {
				var table = query.Access.GetTable(SystemSchema.UserRoleTableName);
				var c1 = table.GetResolvedColumnName(0);

				// All 'user_role' where user = %username%
				var t = table.SimpleSelect(query, c1, SqlExpressionType.Equal, SqlExpression.Constant(Field.String(userName)));
				int sz = t.RowCount;
				var roles = new string[sz];
				var rowEnum = t.GetEnumerator();
				int i = 0;
				while (rowEnum.MoveNext()) {
					roles[i] = t.GetValue(rowEnum.Current.RowId.RowNumber, 1).Value.ToString();
					++i;
				}

				return roles;
			}
		}

		public bool DropUser(string userName) {
			var userExpr = SqlExpression.Constant(Field.String(userName));

			RemoveUserFromAllRoles(userName);

			using (var query = Session.CreateQuery()) {
				// TODO: Remove all object-level privileges from the user...

				//var table = QueryContext.GetMutableTable(SystemSchema.UserConnectPrivilegesTableName);
				//var c1 = table.GetResolvedColumnName(0);
				//var t = table.SimpleSelect(QueryContext, c1, SqlExpressionType.Equal, userExpr);
				//table.Delete(t);

				var table = query.Access.GetMutableTable(SystemSchema.PasswordTableName);
				var c1 = table.GetResolvedColumnName(0);
				var t = table.SimpleSelect(query, c1, SqlExpressionType.Equal, userExpr);
				table.Delete(t);

				table = query.Access.GetMutableTable(SystemSchema.UserTableName);
				c1 = table.GetResolvedColumnName(0);
				t = table.SimpleSelect(query, c1, SqlExpressionType.Equal, userExpr);
				return table.Delete(t) > 0;
			}
		}

		private void RemoveUserFromAllRoles(string username) {
			using (var query = Session.CreateQuery()) {
				var userExpr = SqlExpression.Constant(Field.String(username));

				var table = query.Access.GetMutableTable(SystemSchema.UserRoleTableName);
				var c1 = table.GetResolvedColumnName(0);
				var t = table.SimpleSelect(query, c1, SqlExpressionType.Equal, userExpr);

				if (t.RowCount > 0) {
					table.Delete(t);
				}
			}
		}

		public void AlterUser(UserInfo userInfo, string identifier) {
			using (var query = Session.CreateQuery()) {
				var userName = userInfo.Name;

				var userExpr = SqlExpression.Constant(Field.String(userName));

				// Delete the current username from the 'password' table
				var table = query.Access.GetMutableTable(SystemSchema.PasswordTableName);
				var c1 = table.GetResolvedColumnName(0);
				var t = table.SimpleSelect(query, c1, SqlExpressionType.Equal, userExpr);
				if (t.RowCount != 1)
					throw new SecurityException(String.Format("User '{0}' was not found.", userName));

				table.Delete(t);

				// TODO: get the hash algorithm and hash ...

				var method = userInfo.Identification.Method;
				var methodArgs = SerializeArguments(userInfo.Identification.Arguments);

				if (method != "plain")
					throw new NotImplementedException("Only mechanism implemented right now is plain text (it sucks!)");

				// Add the new username
				table = query.Access.GetMutableTable(SystemSchema.PasswordTableName);
				var row = table.NewRow();
				row.SetValue(0, userName);
				row.SetValue(1, method);
				row.SetValue(2, methodArgs);
				row.SetValue(3, identifier);
				table.AddRow(row);
			}
		}

		public void SetUserStatus(string userName, UserStatus status) {
			using (var query = Session.CreateQuery()) {
				// Internally we implement this by adding the user to the #locked group.
				var table = query.Access.GetMutableTable(SystemSchema.UserRoleTableName);
				var c1 = table.GetResolvedColumnName(0);
				var c2 = table.GetResolvedColumnName(1);

				// All 'user_role' where user = %username%
				var t = table.SimpleSelect(query, c1, SqlExpressionType.Equal, SqlExpression.Constant(userName));

				// All from this set where PrivGroupName = %role%
				t = t.SimpleSelect(query, c2, SqlExpressionType.Equal, SqlExpression.Constant(SystemRoles.LockedRole));

				bool userBelongsToLockGroup = t.RowCount > 0;
				if (status == UserStatus.Locked &&
				    !userBelongsToLockGroup) {
					// Lock the user by adding the user to the Lock group
					// Add this user to the locked group.
					var rdat = new Row(table);
					rdat.SetValue(0, userName);
					rdat.SetValue(1, SystemRoles.LockedRole);
					table.AddRow(rdat);
				} else if (status == UserStatus.Unlocked &&
				           userBelongsToLockGroup) {
					// Unlock the user by removing the user from the Lock group
					// Remove this user from the locked group.
					table.Delete(t);
				}
			}
		}

		public UserStatus GetUserStatus(string userName) {
			if (IsUserInRole(userName, SystemRoles.LockedRole))
				return UserStatus.Locked;

			return UserStatus.Unlocked;
		}

		public UserInfo GetUser(string userName) {
			using (var query = Session.CreateQuery()) {
				var table = query.Access.GetTable(SystemSchema.PasswordTableName);
				var unameColumn = table.GetResolvedColumnName(0);
				var methodColumn = table.GetResolvedColumnName(1);
				var methodArgsColumn = table.GetResolvedColumnName(2);

				var t = table.SimpleSelect(query, unameColumn, SqlExpressionType.Equal, SqlExpression.Constant(userName));
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
		}

		public bool CheckIdentifier(string userName, string identifier) {
			using (var query = Session.CreateQuery()) {
				var table = query.Access.GetTable(SystemSchema.PasswordTableName);
				var unameColumn = table.GetResolvedColumnName(0);
				var idColumn = table.GetResolvedColumnName(3);

				var t = table.SimpleSelect(query, unameColumn, SqlExpressionType.Equal, SqlExpression.Constant(userName));
				if (t.RowCount == 0)
					throw new SecurityException(String.Format("User '{0}' is not registered.", userName));

				var stored = t.GetValue(0, idColumn);
				return stored.Value.ToString().Equals(identifier);
			}
		}


		public void CreateRole(string roleName) {
			if (String.IsNullOrEmpty(roleName))
				throw new ArgumentNullException("roleName");

			var c = roleName[0];
			if (c == '$' || c == '%' || c == '@')
				throw new ArgumentException(String.Format("Group name '{0}' starts with an invalid character.", roleName));

			using (var query = Session.CreateQuery()) {
				var table = query.Access.GetMutableTable(SystemSchema.RoleTableName);

				var row = table.NewRow();
				row.SetValue(0, roleName);

				table.AddRow(row);
			}
		}

		public bool DropRole(string roleName) {
			using (var query = Session.CreateQuery()) {
				var table = query.Access.GetMutableTable(SystemSchema.UserRoleTableName);
				var c1 = table.GetResolvedColumnName(0);

				// All password where name = %roleName%
				var t = table.SimpleSelect(query, c1, SqlExpressionType.Equal, SqlExpression.Constant(roleName));

				if (t.RowCount > 0) {
					table.Delete(t);
					return true;
				}

				return false;
			}
		}

		public bool RoleExists(string roleName) {
			using (var query = Session.CreateQuery()) {
				var table = query.Access.GetTable(SystemSchema.RoleTableName);
				var c1 = table.GetResolvedColumnName(0);

				// All password where name = %roleName%
				var t = table.SimpleSelect(query, c1, SqlExpressionType.Equal, SqlExpression.Constant(roleName));
				return t.RowCount > 0;
			}
		}

		public void AddUserToRole(string userName, string roleName, bool asAdmin) {
			if (String.IsNullOrEmpty(roleName))
				throw new ArgumentNullException("roleName");
			if (String.IsNullOrEmpty(userName))
				throw new ArgumentNullException("userName");

			char c = roleName[0];
			if (c == '@' || c == '&' || c == '#' || c == '$')
				throw new ArgumentException(String.Format("Group name '{0}' is invalid: cannot start with {1}", roleName, c), "roleName");

			if (!IsUserInRole(userName, roleName)) {
				using (var query = Session.CreateQuery()) {
					var table = query.Access.GetMutableTable(SystemSchema.UserRoleTableName);
					var row = table.NewRow();
					row.SetValue(0, userName);
					row.SetValue(1, roleName);
					row.SetValue(2, asAdmin);
					table.AddRow(row);
				}
			}
		}

		public bool IsUserRoleAdmin(string userName, string roleName) {
			using (var query = Session.CreateQuery()) {
				// This is a special query that needs to access the lowest level of ITable, skipping
				// other security controls
				var table = query.Access.GetTable(SystemSchema.UserRoleTableName);
				var c1 = table.GetResolvedColumnName(0);
				var c2 = table.GetResolvedColumnName(1);

				var t = table.SimpleSelect(query, c1, SqlExpressionType.Equal, SqlExpression.Constant(userName));

				t = t.SimpleSelect(query, c2, SqlExpressionType.Equal, SqlExpression.Constant(roleName));
				if (t.RowCount == 0)
					return false;

				return t.GetValue(0, 2).AsBoolean();
			}
		}

		public bool RemoveUserFromRole(string userName, string roleName) {
			using (var query = Session.CreateQuery()) {
				// This is a special query that needs to access the lowest level of ITable, skipping
				// other security controls
				var table = query.Access.GetMutableTable(SystemSchema.UserRoleTableName);
				var c1 = table.GetResolvedColumnName(0);
				var c2 = table.GetResolvedColumnName(1);

				// All 'user_group' where UserName = %username%
				var t = table.SimpleSelect(query, c1, SqlExpressionType.Equal, SqlExpression.Constant(userName));

				// All from this set where PrivGroupName = %group%
				t = t.SimpleSelect(query, c2, SqlExpressionType.Equal, SqlExpression.Constant(roleName));

				if (t.RowCount > 0) {
					table.Delete(t);
					return true;
				}

				return false;
			}
		}

		public bool IsUserInRole(string userName, string roleName) {
			using (var query = Session.CreateQuery()) {
				// This is a special query that needs to access the lowest level of ITable, skipping
				// other security controls
				var table = query.Access.GetTable(SystemSchema.UserRoleTableName);
				var c1 = table.GetResolvedColumnName(0);
				var c2 = table.GetResolvedColumnName(1);

				// All 'user_group' where UserName = %username%
				var t = table.SimpleSelect(query, c1, SqlExpressionType.Equal, SqlExpression.Constant(userName));

				// All from this set where PrivGroupName = %group%
				t = t.SimpleSelect(query, c2, SqlExpressionType.Equal, SqlExpression.Constant(roleName));
				return t.RowCount > 0;
			}
		}

		public string[] GetUserRoles(string userName) {
			return QueryUserRoles(userName);
		}
	}
}
