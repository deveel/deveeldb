// 
//  Copyright 2010-2014 Deveel
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

using System;
using System.Data;

using Deveel.Data.Client;
using Deveel.Data.Configuration;
using Deveel.Data.DbSystem;
using Deveel.Data.Protocol;
using Deveel.Data.Threading;
using Deveel.Data.Transactions;
using Deveel.Diagnostics;

using DataRow = Deveel.Data.DbSystem.DataRow;
using DataTable = Deveel.Data.DbSystem.DataTable;

namespace Deveel.Data.Security {
	public sealed class UserManager {
		public UserManager(IDatabase database) {
			Database = database;
		}

		public IDatabase Database { get; private set; }

		private ILogger Logger {
			get { return Database.Context.Logger; }
		}

		/// <summary>
		/// Performs check to determine if user is allowed access from the given
		/// host.
		/// </summary>
		/// <param name="queryContext"></param>
		/// <param name="username">The name of the user to check the host for.</param>
		/// <param name="endPoint">The end point from where the user attempts to connect.</param>
		/// <returns>
		/// Returns <b>true</b> if the user identified by the given <paramref name="username"/>
		/// is allowed to access for the host specified in the <paramref name="endPoint"/>,
		/// otherwise <b>false</b>.
		/// </returns>
		private bool UserAllowedAccessFromHost(IQueryContext queryContext, string username, ConnectionEndPoint endPoint) {
			// The system user is not allowed to login
			if (username.Equals(User.SystemName))
				return false;

			// What's the protocol?
			string protocol = endPoint.Protocol;
			string host = endPoint.Address;

			if (Logger.IsInterestedIn(LogLevel.Info)) {
				Logger.Info(this, "Checking host: protocol = " + protocol + ", host = " + host);
			}

			// The table to check
			DataTable connectPriv = (DataTable)queryContext.GetTable(SystemSchema.UserConnectPrivileges);
			VariableName unCol = connectPriv.GetResolvedVariable(0);
			VariableName protoCol = connectPriv.GetResolvedVariable(1);
			VariableName hostCol = connectPriv.GetResolvedVariable(2);
			VariableName accessCol = connectPriv.GetResolvedVariable(3);
			// Query: where UserName = %username%
			Table t = connectPriv.SimpleSelect(queryContext, unCol, Operator.Equal,
												new Expression(TObject.CreateString(username)));
			// Query: where %protocol% like Protocol
			Expression exp = Expression.Simple(TObject.CreateString(protocol), Operator.Like, protoCol);
			t = t.ExhaustiveSelect(queryContext, exp);
			// Query: where %host% like Host
			exp = Expression.Simple(TObject.CreateString(host), Operator.Like, hostCol);
			t = t.ExhaustiveSelect(queryContext, exp);

			// Those that are DENY
			Table t2 = t.SimpleSelect(queryContext, accessCol, Operator.Equal,
									  new Expression(TObject.CreateString("DENY")));
			if (t2.RowCount > 0)
				return false;

			// Those that are ALLOW
			Table t3 = t.SimpleSelect(queryContext, accessCol, Operator.Equal,
									  new Expression(TObject.CreateString("ALLOW")));
			if (t3.RowCount > 0)
				return true;

			// No DENY or ALLOW entries for this host so deny access.
			return false;
		}

		/// <summary>
		/// Checks if the user in the context given belongs to secure group.
		/// </summary>
		/// <param name="queryContext"></param>
		/// <returns>
		/// Returns <b>true</b> if the user belongs to the secure access
		/// privileges group, otherwise <b>false</b>.
		/// </returns>
		private bool UserHasSecureAccess(IQueryContext queryContext) {
			// The internal secure user has full privs on everything
			if (queryContext.UserName.Equals(User.SystemName))
				return true;

			return UserBelongsToGroup(queryContext, queryContext.UserName, SystemGroupNames.SecureGroup);
		}

		/// <summary>
		/// Checks if the given user is permitted the given grant for
		/// executing operations on the given schema.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="schema"></param>
		/// <param name="grant"></param>
		/// <returns>
		/// Returns <b>true</b> if the grant manager permits a schema 
		/// operation (eg, <i>CREATE</i>, <i>ALTER</i> and <i>DROP</i> 
		/// table operations) for the given user, otherwise <b>false</b>.
		/// </returns>
		private static bool UserHasSchemaGrant(IQueryContext context, string schema, int grant) {
			// The internal secure user has full privs on everything
			if (context.UserName.Equals(User.SystemName))
				return true;

			// No users have schema access to the system schema.
			if (schema.Equals(SystemSchema.Name))
				return false;

			// Ask the grant manager if there are any privs setup for this user on the
			// given schema.
			Privileges privs = context.GetUserGrants(GrantObject.Schema, schema);

			return privs.Permits(grant);
		}

		/// <summary>
		/// Checks if the given user is permitted the given grant for
		/// executing operations on the given table.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="tableName"></param>
		/// <param name="columns"></param>
		/// <param name="grant"></param>
		/// <returns>
		/// Returns <b>true</b> if the grant manager permits a schema 
		/// operation (eg, <c>CREATE</c>, <c>ALTER</c> and <c>DROP</c> 
		/// table operations) for the given user, otherwise <b>false</b>.
		/// </returns>
		private static bool UserHasTableObjectGrant(IQueryContext context, TableName tableName, VariableName[] columns, int grant) {
			// The internal secure user has full privs on everything
			if (context.UserName.Equals(User.SystemName))
				return true;

			// TODO: Support column level privileges.

			// Ask the grant manager if there are any privs setup for this user on the
			// given schema.
			Privileges privs = context.GetUserGrants(GrantObject.Table, tableName.ToString());

			return privs.Permits(grant);
		}

		/// <summary>
		/// Tries to authenticate a username/password against this database.
		/// </summary>
		/// <remarks>
		/// If a valid object is returned, the user will be logged into 
		/// the engine via the <see cref="LoggedUsers"/>. The developer must 
		/// ensure that <see cref="Dispose()"/> is called before the object is 
		/// disposed (logs out of the system).
		/// <para>
		/// This method also returns <b>null</b> if a user exists but was 
		/// denied access from the given host string. The given <i>host name</i>
		/// is formatted in the database host connection encoding. This 
		/// method checks all the values from the <see cref="SystemSchema.UserPrivileges"/> 
		/// table for this user for the given protocol.
		/// It first checks if the user is specifically <b>denied</b> access 
		/// from the given host.It then checks if the user is <b>allowed</b> 
		/// access from the given host. If a host is neither allowed or denied 
		/// then it is denied.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="User"/> object if the given user was authenticated 
		/// successfully, otherwise <b>null</b>.
		/// </returns>
		public User AuthenticateUser(string username, string password, ConnectionEndPoint endPoint) {
			// Create a temporary connection for authentication only...
			IDatabaseConnection connection = Database.CreateNewConnection(null, null);
			var queryContext = new DatabaseQueryContext(connection);
			connection.CurrentSchema = SystemSchema.Name;
			LockingMechanism locker = connection.LockingMechanism;
			locker.SetMode(LockingMode.Exclusive);
			try {
				try {
					var table = queryContext.GetTable(SystemSchema.Password);
					VariableName unameColumn = table.GetResolvedVariable(0);
					VariableName passwColumn = table.GetResolvedVariable(1);
					VariableName saltColumn = table.GetResolvedVariable(2);
					VariableName hashColumn = table.GetResolvedVariable(3);

					Table t = table.SimpleSelect(queryContext, unameColumn, Operator.Equal, new Expression(TObject.CreateString(username)));
					if (t.RowCount == 0)
						return null;

					var pass = t.GetCell(passwColumn, 0);
					var salt = t.GetCell(saltColumn, 0);
					var hash = t.GetCell(hashColumn, 0);

					if (pass == null || salt == null || hash == null)
						return null;

					var crypto = PasswordCrypto.Parse(hash);
					if (!crypto.Verify(pass, password, salt))
						return null;

					// Now check if this user is permitted to connect from the given
					// host.
					if (UserAllowedAccessFromHost(queryContext, username, endPoint)) {
						// Successfully authenticated...
						User user = new User(username, Database, endPoint.ToString(), DateTime.Now);
						// Log the authenticated user in to the engine.
						Database.Context.LoggedUsers.OnUserLoggedIn(user);
						return user;
					}

					return null;
				} catch (DataException e) {
					Logger.Error(this, e);
					throw new DatabaseException("Data Error: " + e.Message, e);
				}
			} finally {
				try {
					// Make sure we commit the connection.
					connection.Commit();
				} catch (TransactionException e) {
					// Just issue a warning...
					Logger.Warning(this, e);
				} finally {
					// Guarentee that we unluck from EXCLUSIVE
					locker.FinishMode(LockingMode.Exclusive);
				}
				// And make sure we close (dispose) of the temporary connection.
				connection.Close();
			}
		}

		/// <summary>
		/// Checks if a user exists within the database.
		/// </summary>
		/// <param name="queryContext">The queryContext of the session.</param>
		/// <param name="username">The name of the user to check.</param>
		/// <remarks>
		/// <b>Note:</b> Assumes exclusive Lock on the session.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if the user identified by the given 
		/// <paramref name="username"/>, otherwise <b>false</b>.
		/// </returns>
		public bool UserExists(IQueryContext queryContext, String username) {
			Table table = queryContext.GetTable(SystemSchema.Password);
			VariableName c1 = table.GetResolvedVariable(0);
			// All password where UserName = %username%
			Table t = table.SimpleSelect(queryContext, c1, Operator.Equal, new Expression(TObject.CreateString(username)));
			return t.RowCount > 0;
		}

		/// <summary>
		/// Creates a new user for the database.
		/// </summary>
		/// <param name="queryContext"></param>
		/// <param name="username">The name of the user to create.</param>
		/// <param name="password">The user password.</param>
		/// <remarks>
		/// <b>Note</b>: Assumes exclusive Lock on <see cref="DatabaseConnection"/>.
		/// </remarks>
		/// <exception cref="DatabaseException">
		/// If the user is already defined by the database
		/// </exception>
		public void CreateUser(IQueryContext queryContext, string username, string password) {
			if (username == null || password == null)
				throw new DatabaseException("Username or password can not be NULL.");

			// The username must be more than 1 character
			if (username.Length <= 1)
				throw new DatabaseException("Username must be at least 2 characters.");

			// The password must be more than 1 character
			if (password.Length <= 1)
				throw new DatabaseException("Password must be at least 2 characters.");

			// Check the user doesn't already exist
			if (UserExists(queryContext, username))
				throw new DatabaseException("User '" + username + "' already exists.");

			// Some usernames are reserved words
			if (String.Compare(username, "public", StringComparison.OrdinalIgnoreCase) == 0)
				throw new DatabaseException("User '" + username + "' not allowed - reserved.");

			// Usernames starting with @, &, # and $ are reserved for system
			// identifiers
			char c = username[0];
			if (c == '@' || c == '&' || c == '#' || c == '$') {
				throw new DatabaseException("User name can not start with '" + c +
											"' character.");
			}

			var hashFuncDef = Database.Context.Config.PasswordHashFunction();
			var crypto = PasswordCrypto.Parse(hashFuncDef);

			string salt;
			password = crypto.Hash(password, out salt);

			// Add this user to the password table.
			DataTable table = (DataTable)queryContext.GetTable(SystemSchema.Password);
			DataRow rdat = new DataRow(table);
			rdat.SetValue(0, username);
			rdat.SetValue(1, password);
			rdat.SetValue(2, salt);
			rdat.SetValue(3, crypto.ToString());
			table.Add(rdat);
		}

		/// <summary>
		/// Deletes all the groups the user belongs to.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="username"></param>
		/// <remarks>
		/// This is intended for a user alter command for setting the groups 
		/// a user belongs to.
		/// <para>
		/// <b>Note:</b> Assumes exclusive Lock on database session.
		/// </para>
		/// </remarks>
		public void DeleteAllUserGroups(IQueryContext context, string username) {
			Expression userExpr = new Expression(TObject.CreateString(username));

			DataTable table = (DataTable)context.GetTable(SystemSchema.UserPrivileges);
			VariableName c1 = table.GetResolvedVariable(0);
			// All 'user_priv' where UserName = %username%
			Table t = table.SimpleSelect(context, c1, Operator.Equal, userExpr);
			// Delete all the groups
			table.Delete(t);
		}

		/// <summary>
		/// Drops a user from the database.
		/// </summary>
		/// <param name="queryContext"></param>
		/// <param name="username">The name of the user to drop.</param>
		/// <remarks>
		/// This also deletes all information associated with a user such as 
		/// the groups they belong to. It does not delete the privs a user 
		/// has set up.
		/// <para>
		/// <b>Note:</b> Assumes exclusive Lock on database session.
		/// </para>
		/// </remarks>
		public void DeleteUser(IQueryContext queryContext, string username) {
			// TODO: This should check if there are any tables the user has setup
			//  and not allow the delete if there are.
			Expression userExpr = new Expression(TObject.CreateString(username));

			// First delete all the groups from the user priv table
			DeleteAllUserGroups(queryContext, username);

			// Now delete the username from the user_connect_priv table
			DataTable table = (DataTable)queryContext.GetTable(SystemSchema.UserConnectPrivileges);
			VariableName c1 = table.GetResolvedVariable(0);
			Table t = table.SimpleSelect(queryContext, c1, Operator.Equal, userExpr);
			table.Delete(t);

			// Finally delete the username from the 'password' table
			table = (DataTable)queryContext.GetTable(SystemSchema.Password);
			c1 = table.GetResolvedVariable(0);
			t = table.SimpleSelect(queryContext, c1, Operator.Equal, userExpr);
			table.Delete(t);
		}

		/// <summary>
		/// Alters the password of the given user.
		/// </summary>
		/// <param name="queryContext"></param>
		/// <param name="username">The name of the user to alter the password.</param>
		/// <param name="password">The new password for the user.</param>
		/// <remarks>
		/// <b>Note:</b> Assumes exclusive Lock on database session.
		/// </remarks>
		public void AlterUserPassword(IQueryContext queryContext, string username, string password) {
			Expression userExpr = new Expression(TObject.CreateString(username));

			// Delete the current username from the 'password' table
			DataTable table = (DataTable)queryContext.GetTable(SystemSchema.Password);
			VariableName c1 = table.GetResolvedVariable(0);
			Table t = table.SimpleSelect(queryContext, c1, Operator.Equal, userExpr);
			if (t.RowCount != 1)
				throw new DatabaseException("Username '" + username + "' was not found.");

			table.Delete(t);

			var hashFuncDef = Database.Context.Config.PasswordHashFunction();
			var crypto = PasswordCrypto.Parse(hashFuncDef);

			string salt;
			password = crypto.Hash(password, out salt);

			// Add the new username
			table = (DataTable)queryContext.GetTable(SystemSchema.Password);
			DataRow rdat = new DataRow(table);
			rdat.SetValue(0, username);
			rdat.SetValue(1, password);
			rdat.SetValue(2, salt);
			rdat.SetValue(3, crypto.ToString());
			table.Add(rdat);
		}

		/// <summary>
		/// Returns the list of all user groups the user belongs to.
		/// </summary>
		/// <param name="queryContext"></param>
		/// <param name="username"></param>
		/// <returns></returns>
		public string[] GroupsUserBelongsTo(IQueryContext queryContext, String username) {
			Table table = queryContext.GetTable(SystemSchema.UserPrivileges);
			VariableName c1 = table.GetResolvedVariable(0);
			// All 'user_priv' where UserName = %username%
			Table t = table.SimpleSelect(queryContext, c1, Operator.Equal, new Expression(TObject.CreateString(username)));
			int sz = t.RowCount;
			string[] groups = new string[sz];
			IRowEnumerator rowEnum = t.GetRowEnumerator();
			int i = 0;
			while (rowEnum.MoveNext()) {
				groups[i] = t.GetCell(1, rowEnum.RowIndex).Object.ToString();
				++i;
			}

			return groups;
		}

		/// <summary>
		/// Checks if a user belongs in a specified group.
		/// </summary>
		/// <param name="queryContext"></param>
		/// <param name="username">The name of the user to check.</param>
		/// <param name="group">The name of the group to check.</param>
		/// <remarks>
		/// <b>Note</b> Assumes exclusive Lock on database session.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if the given user belongs to the given
		/// <paramref name="group"/>, otherwise <b>false</b>.
		/// </returns>
		public bool UserBelongsToGroup(IQueryContext queryContext, string username, string group) {
			Table table = queryContext.GetTable(SystemSchema.UserPrivileges);
			VariableName c1 = table.GetResolvedVariable(0);
			VariableName c2 = table.GetResolvedVariable(1);
			// All 'user_priv' where UserName = %username%
			Table t = table.SimpleSelect(queryContext, c1, Operator.Equal, new Expression(TObject.CreateString(username)));
			// All from this set where PrivGroupName = %group%
			t = t.SimpleSelect(queryContext, c2, Operator.Equal, new Expression(TObject.CreateString(group)));
			return t.RowCount > 0;
		}

		/// <summary>
		/// Adds a user to the given group.
		/// </summary>
		/// <param name="queryContext"></param>
		/// <param name="username">The name of the user to be added.</param>
		/// <param name="group">The name of the group to add the user to.</param>
		/// <remarks>
		/// This makes an entry in the <see cref="SystemSchema.UserPrivileges"/> for this user 
		/// and the given group.
		/// If the user already belongs to the group then no changes are made.
		/// <para>
		/// It is important that any security checks for ensuring the grantee is
		/// allowed to give the user these privileges are preformed before this 
		/// method is called.
		/// </para>
		/// <para>
		/// <b>Note</b> Assumes exclusive Lock on database session.
		/// </para>
		/// </remarks>
		/// <exception cref="DatabaseException">
		/// If the group name is not properly formatted.
		/// </exception>
		public void AddUserToGroup(IQueryContext queryContext, string username, string group) {
			if (group == null)
				throw new DatabaseException("Can add NULL group.");

			// Groups starting with @, &, # and $ are reserved for system
			// identifiers
			char c = group[0];
			if (c == '@' || c == '&' || c == '#' || c == '$') {
				throw new DatabaseException("The group name can not start with '" + c +
											"' character.");
			}

			// Check the user doesn't belong to the group
			if (!UserBelongsToGroup(queryContext, username, group)) {
				// The user priv table
				DataTable table = (DataTable)queryContext.GetTable(SystemSchema.UserPrivileges);
				// Add this user to the group.
				DataRow rdat = new DataRow(table);
				rdat.SetValue(0, username);
				rdat.SetValue(1, group);
				table.Add(rdat);
			}
			// NOTE: we silently ignore the case when a user already belongs to the
			//   group.
		}

		/// <summary>
		/// Sets the Lock status for the given user.
		/// </summary>
		/// <param name="queryContext"></param>
		/// <param name="lockStatus"></param>
		/// <remarks>
		/// If a user account if locked, it is rejected from logging in 
		/// to the database.
		/// <para>
		/// It is important that any security checks to determine if the process
		/// setting the user Lock is allowed to do it is done before this method is
		/// called.
		/// </para>
		/// <para>
		/// <b>Note:</b> Assumes exclusive Lock on database session.
		/// </para>
		/// </remarks>
		public void SetUserLock(IQueryContext queryContext, bool lockStatus) {
			string username = queryContext.UserName;

			// Internally we implement this by adding the user to the #locked group.
			DataTable table = (DataTable)queryContext.GetTable(SystemSchema.UserPrivileges);
			VariableName c1 = table.GetResolvedVariable(0);
			VariableName c2 = table.GetResolvedVariable(1);
			// All 'user_priv' where UserName = %username%
			Table t = table.SimpleSelect(queryContext, c1, Operator.Equal, new Expression(TObject.CreateString(username)));
			// All from this set where PrivGroupName = %group%
			t = t.SimpleSelect(queryContext, c2, Operator.Equal, new Expression(TObject.CreateString(SystemGroupNames.LockGroup)));

			bool userBelongsToLockGroup = t.RowCount > 0;
			if (lockStatus && !userBelongsToLockGroup) {
				// Lock the user by adding the user to the Lock group
				// Add this user to the locked group.
				DataRow rdat = new DataRow(table);
				rdat.SetValue(0, username);
				rdat.SetValue(1, SystemGroupNames.LockGroup);
				table.Add(rdat);
			} else if (!lockStatus && userBelongsToLockGroup) {
				// Unlock the user by removing the user from the Lock group
				// Remove this user from the locked group.
				table.Delete(t);
			}
		}

		/// <summary>
		/// Grants the given user access to connect to the database from the
		/// given host address.
		/// </summary>
		/// <param name="queryContext"></param>
		/// <param name="user">The name of the user to grant the access to.</param>
		/// <param name="protocol">The connection protocol (either <i>TCP</i> or <i>Local</i>).</param>
		/// <param name="host">The connection host.</param>
		/// <remarks>
		/// We look forward to support more protocols.
		/// </remarks>
		/// <exception cref="DatabaseException">
		/// If the given <paramref name="protocol"/> is not <i>TCP</i> or 
		/// <i>Local</i>.
		/// </exception>
		public void GrantHostAccessToUser(IQueryContext queryContext, string user, string protocol, string host) {
			// The user connect priv table
			DataTable table = (DataTable)queryContext.GetTable(SystemSchema.UserConnectPrivileges);
			// Add the protocol and host to the table
			DataRow rdat = new DataRow(table);
			rdat.SetValue(0, user);
			rdat.SetValue(1, protocol);
			rdat.SetValue(2, host);
			rdat.SetValue(3, "ALLOW");
			table.Add(rdat);
		}

		/// <summary>
		/// Checks if the given user can create users on the database.
		/// </summary>
		/// <param name="context"></param>
		/// <remarks>
		/// Only members of the <i>secure access</i> group, or the 
		/// <i>user manager</i> group can do this.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if the user is permitted to create, 
		/// alter and drop user information from the database, otherwise 
		/// returns <b>false</b>.
		/// </returns>
		public bool CanUserCreateAndDropUsers(IQueryContext context) {
			return (UserHasSecureAccess(context) ||
					UserBelongsToGroup(context, context.UserName, SystemGroupNames.UserManagerGroup));
		}

		/// <summary>
		/// Returns true if the user is permitted to create and drop schema's in the
		/// database, otherwise returns false.
		/// </summary>
		/// <remarks>
		/// Only members of the 'secure access' group, or the 'schema manager' group 
		/// can do this.
		/// </remarks>
		public bool CanUserCreateAndDropSchema(IQueryContext context, string schema) {
			// The internal secure user has full privs on everything
			if (context.UserName.Equals(User.SystemName))
				return true;

			// No user can create or drop the system schema.
			if (schema.Equals(SystemSchema.Name))
				return false;

			return (UserHasSecureAccess(context) ||
					UserBelongsToGroup(context, context.UserName, SystemGroupNames.SchemaManagerGroup));
		}

		/// <summary>
		/// Returns true if the user can shut down the database server.
		/// </summary>
		/// <remarks>
		/// A user can shut down the database if they are a member of the 
		/// 'secure acces' group.
		/// </remarks>
		public bool CanUserShutDown(IQueryContext context) {
			return UserHasSecureAccess(context);
		}

		/// <summary>
		/// Returns true if the user is allowed to execute the given stored procedure.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="procedureName"></param>
		/// <returns></returns>
		public bool CanUserExecuteStoredProcedure(IQueryContext context, string procedureName) {
			// Currently you can only execute a procedure if you are a member of the
			// secure access priv group.
			return UserHasSecureAccess(context);
		}

		// ---- General schema level privs ----

		/// <summary>
		/// Returns true if the user can create a table or view with the given name,
		/// otherwise returns false.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		public bool CanUserCreateTableObject(IQueryContext context, TableName table) {
			if (UserHasSchemaGrant(context, table.Schema, Privileges.Create))
				return true;

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context);
		}

		/// <summary>
		/// Returns true if the user can alter a table or view with the given name,
		/// otherwise returns false.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		public bool CanUserAlterTableObject(IQueryContext context, TableName table) {
			if (UserHasSchemaGrant(context, table.Schema, Privileges.Alter))
				return true;

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context);
		}

		/// <summary>
		/// Returns true if the user can drop a table or view with the given name,
		/// otherwise returns false.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		public bool CanUserDropTableObject(IQueryContext context, TableName table) {
			if (UserHasSchemaGrant(context, table.Schema, Privileges.Drop))
				return true;

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context);
		}

		// ---- Check table object privs ----

		/// <summary>
		/// Returns true if the user can select from a table or view with the given
		/// name and given columns, otherwise returns false.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="table"></param>
		/// <param name="columns"></param>
		/// <returns></returns>
		public bool CanUserSelectFromTableObject(IQueryContext context, TableName table, VariableName[] columns) {
			if (UserHasTableObjectGrant(context, table, columns, Privileges.Select))
				return true;

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context);
		}

		/// <summary>
		///  Returns true if the user can insert into a table or view with the given
		/// name and given columns, otherwise returns false.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="table"></param>
		/// <param name="columns"></param>
		/// <returns></returns>
		public bool CanUserInsertIntoTableObject(IQueryContext context, TableName table, VariableName[] columns) {
			if (UserHasTableObjectGrant(context, table, columns, Privileges.Insert))
				return true;

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context);
		}

		/// <summary>
		/// Returns true if the user can update a table or view with the given
		/// name and given columns, otherwise returns false.
		/// </summary>
		public bool CanUserUpdateTableObject(IQueryContext context, TableName table, VariableName[] columns) {
			if (UserHasTableObjectGrant(context, table, columns, Privileges.Update))
				return true;

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context);
		}

		///<summary>
		/// Returns true if the user can delete from a table or view with the given
		/// name and given columns, otherwise returns false.
		///</summary>
		///<param name="context"></param>
		///<param name="table"></param>
		///<returns></returns>
		public bool CanUserDeleteFromTableObject(IQueryContext context, TableName table) {
			if (UserHasTableObjectGrant(context, table, null, Privileges.Delete))
				return true;

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context);
		}

		///<summary>
		/// Returns true if the user can compact a table with the given name,
		/// otherwise returns false.
		///</summary>
		///<param name="context"></param>
		///<param name="table"></param>
		///<returns></returns>
		public bool CanUserCompactTableObject(IQueryContext context, TableName table) {
			if (UserHasTableObjectGrant(context, table, null, Privileges.Compact))
				return true;

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context);
		}

		///<summary>
		/// Returns true if the user can create a procedure with the given name,
		/// otherwise returns false.
		///</summary>
		///<param name="context"></param>
		///<param name="table"></param>
		///<returns></returns>
		public bool CanUserCreateProcedureObject(IQueryContext context, TableName table) {
			if (UserHasSchemaGrant(context, table.Schema, Privileges.Create))
				return true;

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context);
		}

		///<summary>
		/// Returns true if the user can drop a procedure with the given name,
		/// otherwise returns false.
		///</summary>
		///<param name="context"></param>
		///<param name="table"></param>
		///<returns></returns>
		public bool CanUserDropProcedureObject(IQueryContext context, TableName table) {
			if (UserHasSchemaGrant(context, table.Schema, Privileges.Drop))
				return true;

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context);
		}

		///<summary>
		///  Returns true if the user can create a sequence with the given name,
		/// otherwise returns false.
		///</summary>
		///<param name="context"></param>
		///<param name="table"></param>
		///<returns></returns>
		public bool CanUserCreateSequenceObject(IQueryContext context, TableName table) {
			if (UserHasSchemaGrant(context, table.Schema, Privileges.Create))
				return true;

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context);
		}

		///<summary>
		/// Returns true if the user can drop a sequence with the given name,
		/// otherwise returns false.
		///</summary>
		///<param name="context"></param>
		///<param name="table"></param>
		///<returns></returns>
		public bool CanUserDropSequenceObject(IQueryContext context, TableName table) {
			if (UserHasSchemaGrant(context, table.Schema, Privileges.Drop))
				return true;

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context);
		}
	}
}