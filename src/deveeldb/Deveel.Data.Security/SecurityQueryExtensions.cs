using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Protocol;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Transactions;


namespace Deveel.Data.Security {
	public static class SecurityQueryExtensions {
		private static bool UserHasSecureAccess(this IQueryContext queryContext) {
			// The internal secure user has full privs on everything
			if (queryContext.User().IsSystem)
				return true;

			return UserBelongsToGroup(queryContext, queryContext.User().Name, SystemGroupNames.SecureGroup);
		}

		private static bool UserAllowedAccessFromHost(this IQueryContext queryContext, string username, ConnectionEndPoint endPoint) {
			// The system user is not allowed to login
			if (username.Equals(User.SystemName))
				return false;

			// What's the protocol?
			string protocol = endPoint.Protocol;
			string host = endPoint.Address;

			// The table to check
			var connectPriv = queryContext.GetTable(SystemSchema.UserConnectPrivilegesTableName);
			var unCol = connectPriv.GetResolvedColumnName(0);
			var protoCol = connectPriv.GetResolvedColumnName(1);
			var hostCol = connectPriv.GetResolvedColumnName(2);
			var accessCol = connectPriv.GetResolvedColumnName(3);
			// Query: where UserName = %username%
			var t = connectPriv.SimpleSelect(queryContext, unCol, BinaryOperator.Equal, SqlExpression.Constant(username));
			// Query: where %protocol% like Protocol
			var exp = SqlExpression.Binary(SqlExpression.Constant(protocol), SqlExpressionType.Like,
				SqlExpression.Reference(protoCol));
			t = t.ExhaustiveSelect(queryContext, exp);
			// Query: where %host% like Host
			exp = SqlExpression.Binary(SqlExpression.Constant(host), SqlExpressionType.Like, SqlExpression.Reference(hostCol));
			t = t.ExhaustiveSelect(queryContext, exp);

			// Those that are DENY
			var t2 = t.SimpleSelect(queryContext, accessCol, BinaryOperator.Equal, SqlExpression.Constant("DENY"));
			if (t2.RowCount > 0)
				return false;

			// Those that are ALLOW
			var t3 = t.SimpleSelect(queryContext, accessCol, BinaryOperator.Equal, SqlExpression.Constant("ALLOW"));
			if (t3.RowCount > 0)
				return true;

			// No DENY or ALLOW entries for this host so deny access.
			return false;
		}

		public static bool UserBelongsToGroup(this IQueryContext queryContext, string username, string group) {
			var table = queryContext.GetTable(SystemSchema.UserPrivilegesTableName);
			var c1 = table.GetResolvedColumnName(0);
			var c2 = table.GetResolvedColumnName(1);
			// All 'user_priv' where UserName = %username%
			var t = table.SimpleSelect(queryContext, c1, BinaryOperator.Equal, SqlExpression.Constant(username));
			// All from this set where PrivGroupName = %group%
			t = t.SimpleSelect(queryContext, c2, BinaryOperator.Equal, SqlExpression.Constant(group));
			return t.RowCount > 0;
		}

		public static void AddUserToGroup(this IQueryContext queryContext, string username, string group) {
			if (group == null)
				throw new DatabaseSystemException("Can add NULL group.");

			// Groups starting with @, &, # and $ are reserved for system
			// identifiers
			char c = group[0];
			if (c == '@' || c == '&' || c == '#' || c == '$') {
				throw new DatabaseSystemException("The group name can not start with '" + c +
											"' character.");
			}

			// Check the user doesn't belong to the group
			if (!UserBelongsToGroup(queryContext, username, group)) {
				// The user priv table
				var table = queryContext.GetMutableTable(SystemSchema.UserPrivilegesTableName);
				// Add this user to the group.
				var rdat = new Row(table);
				rdat.SetValue(0, username);
				rdat.SetValue(1, group);
				table.AddRow(rdat);
			}
			// NOTE: we silently ignore the case when a user already belongs to the
			//   group.
		}

		public static void SetUserLock(this IQueryContext queryContext, string username, bool lockStatus) {
			// Internally we implement this by adding the user to the #locked group.
			var table = queryContext.GetMutableTable(SystemSchema.UserPrivilegesTableName);
			var c1 = table.GetResolvedColumnName(0);
			var c2 = table.GetResolvedColumnName(1);
			// All 'user_priv' where UserName = %username%
			var t = table.SimpleSelect(queryContext, c1, BinaryOperator.Equal, SqlExpression.Constant(username));
			// All from this set where PrivGroupName = %group%
			t = t.SimpleSelect(queryContext, c2, BinaryOperator.Equal, SqlExpression.Constant(SystemGroupNames.LockGroup));

			bool userBelongsToLockGroup = t.RowCount > 0;
			if (lockStatus && !userBelongsToLockGroup) {
				// Lock the user by adding the user to the Lock group
				// Add this user to the locked group.
				var rdat = new Row(table);
				rdat.SetValue(0, username);
				rdat.SetValue(1, SystemGroupNames.LockGroup);
				table.AddRow(rdat);
			} else if (!lockStatus && userBelongsToLockGroup) {
				// Unlock the user by removing the user from the Lock group
				// Remove this user from the locked group.
				table.Delete(t);
			}
		}

		public static void GrantHostAccessToUser(this IQueryContext queryContext, string user, string protocol, string host) {
			// The user connect priv table
			var table = queryContext.GetMutableTable(SystemSchema.UserConnectPrivilegesTableName);
			// Add the protocol and host to the table
			var rdat = new Row(table);
			rdat.SetValue(0, user);
			rdat.SetValue(1, protocol);
			rdat.SetValue(2, host);
			rdat.SetValue(3, "ALLOW");
			table.AddRow(rdat);
		}

		public static bool CanUserCreateAndDropUsers(this IQueryContext context) {
			return (UserHasSecureAccess(context) ||
					UserBelongsToGroup(context, context.User().Name, SystemGroupNames.UserManagerGroup));
		}

		public static User AuthenticateUser(this IDatabase database, string username, string password, ConnectionEndPoint endPoint) {
			// Create a temporary connection for authentication only...
			using (var session = database.CreateSystemSession()) {
				var queryContext = new SessionQueryContext(session);
				session.CurrentSchema(SystemSchema.Name);
				session.ExclusiveLock();

				try {
					try {
						var table = queryContext.GetTable(SystemSchema.PasswordTableName);
						var unameColumn = table.GetResolvedColumnName(0);
						var passwColumn = table.GetResolvedColumnName(1);
						var saltColumn = table.GetResolvedColumnName(2);
						var hashColumn = table.GetResolvedColumnName(3);

						var t = table.SimpleSelect(queryContext, unameColumn, BinaryOperator.Equal, SqlExpression.Constant(username));
						if (t.RowCount == 0)
							return null;

						var pass = t.GetValue(0, passwColumn);
						var salt = t.GetValue(0, saltColumn);
						var hash = t.GetValue(0, hashColumn);

						if (pass == null || salt == null || hash == null)
							return null;

						var crypto = PasswordCrypto.Parse(hash);
						if (!crypto.Verify(pass, password, salt))
							return null;

						// Now check if this user is permitted to connect from the given
						// host.
						if (UserAllowedAccessFromHost(queryContext, username, endPoint))

							// Successfully authenticated...
							return new User(username);

						return null;
					} catch (Exception e) {
						throw new DatabaseSystemException("Data Error: " + e.Message, e);
					}
				} finally {
					try {
						// Make sure we commit the connection.
						session.Commit();
					} catch (TransactionException e) {
						// Just issue a warning...
						//TODO: 
					} finally {
						// Guarentee that we unluck from EXCLUSIVE
						session.ReleaseLocks();
					}
				}
			}
		}
	}
}