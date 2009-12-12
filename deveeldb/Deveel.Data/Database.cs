//  
//  Database.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;
using System.Data;
using System.IO;

using Deveel.Data.Caching;
using Deveel.Data.Client;
using Deveel.Data.Control;
using Deveel.Data.Procedures;
using Deveel.Data.Store;
using Deveel.Diagnostics;

namespace Deveel.Data {
	/// <summary>
	/// The representation of a single database in the system.
	/// </summary>
	/// <remarks>
	/// A database is a set of schema, tables, definitions of tables in 
	/// the schemas, and descriptions of schemas.
	/// <para>
	/// This class encapsulates the top level behaviour of a database. That is
	/// of creating itself, initializing itself, shutting itself down, deleting
	/// itself, creating/dropping a table, updating a table. It is not the 
	/// responsibility of this class to handle table behaviour above this. Top
	/// level table behaviour is handled by <see cref="DataTable"/> through the 
	/// <see cref="DatabaseConnection"/> interface.
	/// </para>
	/// <para>
	/// The <see cref="Database"/> object is also responsible for various 
	/// database management functions such a creating, editing and removing 
	/// users, triggers, functions and services.
	/// </para>
	/// </remarks>
	public sealed class Database : IDisposable {
		// ---------- Statics ----------

		/// <summary>
		/// The username of the internal secure user.
		/// </summary>
		/// <remarks>
		/// The internal secure user is only used for internal highly privileged 
		/// operations. This user is given full privs to everything and is used to 
		/// manage the system tables, for authentication, etc.
		/// </remarks>
		public const String InternalSecureUsername = "@SYSTEM";

		/// <summary>
		/// The name of the lock group.
		/// </summary>
		/// <remarks>
		/// If a user belongs to this group the user account is locked and they are not 
		/// allowed to log into the database.
		/// </remarks>
		public const string LockGroup = "#locked";

		/// <summary>
		/// The name of the schema manager group.
		/// </summary>
		/// <remarks>
		/// Users that belong in this group can create and drop schema from the system.
		/// </remarks>
		public const String SchemaManagerGroup = "schema manager";

		/// <summary>
		/// THe name of the secure access group.
		/// </summary>
		/// <remarks>
		/// If a user belongs to this group they are permitted to perform a number of 
		/// priviledged operations such as shutting down the database, and adding and 
		/// removing users.
		/// </remarks>
		public const string SecureGroup = "secure access";

		/// <summary>
		/// The name of the user manager group.
		/// </summary>
		/// <remarks>
		/// Users that belong in this group can create, alter and drop users from the 
		/// system.
		/// </remarks>
		public const String UserManagerGroup = "user manager";

		/// <summary>
		/// The name of the default schema.
		/// </summary>
		public const String DefaultSchema = "APP";

		/// <summary>
		/// The name of the schema that contains helper tables.
		/// </summary>
		public const String InformationSchema = "INFORMATION_SCHEMA";

		/// <summary>
		/// The NEW table used inside a triggered procedure to represent a triggered
		/// row after the operation occurs.
		/// </summary>
		public static readonly TableName NewTriggerTable = new TableName(SystemSchema, "NEW");

		/// <summary>
		/// The OLD table used inside a triggered procedure to represent a triggered
		/// row before the operation occurs.
		/// </summary>
		public static readonly TableName OldTriggerTable = new TableName(SystemSchema, "OLD");

		/// <summary>
		/// The system internally generated 'sUSRDataTrigger' table.
		/// </summary>
		public static readonly TableName SysDataTrigger = new TableName(SystemSchema, "sUSRDataTrigger");

		/// <summary>
		/// The system internally generated 'sUSRDatabaseStatistics' table.
		/// </summary>
		public static readonly TableName SysDbStatistics = new TableName(SystemSchema, "sUSRDatabaseStatistics");

		/// <summary>
		/// The function table.
		/// </summary>
		public static readonly TableName SysFunction = new TableName(SystemSchema, "sUSRFunction");

		/// <summary>
		/// The function factory table.
		/// </summary>
		public static readonly TableName SysFunctionfactory = new TableName(SystemSchema, "sUSRFunctionFactory");

		///<summary>
		///</summary>
		public static readonly TableName SysGrants = new TableName(SystemSchema, "sUSRGrant");

		/// <summary>
		/// The label table.
		/// </summary>
		public static readonly TableName SysLabel = new TableName(SystemSchema, "sUSRLabel");

		/// <summary>
		/// The password privs and grants table.
		/// </summary>
		public static readonly TableName SysPassword = new TableName(SystemSchema, "sUSRPassword");

		/// <summary>
		/// The services table.
		/// </summary>
		public static readonly TableName SysService = new TableName(SystemSchema, "sUSRService");

		/// <summary>
		/// The system internally generated 'sUSRTableColumns' table.
		/// </summary>
		public static readonly TableName SysTableColumns = new TableName(SystemSchema, "sUSRTableColumns");

		/// <summary>
		/// The system internally generated 'sUSRTableInfo' table.
		/// </summary>
		public static readonly TableName SysTableInfo = new TableName(SystemSchema, "sUSRTableInfo");

		///<summary>
		///</summary>
		public static readonly TableName SysUserconnect = new TableName(SystemSchema, "sUSRUserConnectPriv");

		///<summary>
		///</summary>
		public static readonly TableName SysUserpriv = new TableName(SystemSchema, "sUSRUserPriv");

		/// <summary>
		/// The view table.
		/// </summary>
		public static readonly TableName SysView = new TableName(SystemSchema, "sUSRView");

		/// <summary>
		/// The name of the system schema that contains tables refering to 
		/// system information.
		/// </summary>
		public const String SystemSchema = TableDataConglomerate.SystemSchema;

		/// <summary>
		/// The TableDataConglomerate that contains the conglomerate of tables for
		/// this database.
		/// </summary>
		private readonly TableDataConglomerate conglomerate;

		/// <summary>
		/// An internal secure User that is given full grant access to the entire
		/// database.  This user is used to execute system level queries such as
		/// creating and updating system tables.
		/// </summary>
		private readonly User internal_system_user;

		/// <summary>
		/// The name of this database.
		/// </summary>
		private readonly String name;

		/// <summary>
		/// A table that has a single row but no columns.
		/// </summary>
		private readonly Table single_row_table;

		/// <summary>
		/// The DatabaseSystem that this database is part of.
		/// </summary>
		private readonly DatabaseSystem system;

		/// <summary>
		/// The database wide TriggerManager object that dispatches trigger events
		/// to the DatabaseConnection objects that are listening for the events.
		/// </summary>
		private readonly TriggerManager trigger_manager;

		/// <summary>
		/// This log file records the SQL commands executed on the server.
		/// </summary>
		private Log commands_log;

		/// <summary>
		/// A flag which, when set to true, will cause the engine to delete the
		/// database from the file system when it is shut down.
		/// </summary>
		private bool delete_on_shutdown;

		/// <summary>
		/// This is set to true when the 'init()' method is first called.
		/// </summary>
		private bool initialised;

		///<summary>
		///</summary>
		///<param name="system"></param>
		///<param name="name"></param>
		public Database(DatabaseSystem system, String name) {
			this.system = system;
			delete_on_shutdown = false;
			this.name = name;
			system.RegisterDatabase(this);
			conglomerate = new TableDataConglomerate(system, system.StoreSystem);
			internal_system_user = new User(InternalSecureUsername, this, "", DateTime.Now);

			// Create the single row table
			TemporaryTable t = new TemporaryTable(this,"SINGLE_ROW_TABLE", new DataTableColumnDef[0]);
			t.NewRow();
			single_row_table = t;

			trigger_manager = new TriggerManager(system);
		}

		/// <summary>
		/// Returns the name of this database.
		/// </summary>
		public string Name {
			get { return name; }
		}

		/// <summary>
		/// Returns true if this database is in read-only mode.
		/// </summary>
		public bool IsReadOnly {
			get { return System.ReadOnlyAccess; }
		}

		/// <summary>
		/// Returns the internal system user for this database.
		/// </summary>
		private User InternalSystemUser {
			get { return internal_system_user; }
		}

		// ---------- Log accesses ----------

		/// <summary>
		/// Returns the log file where commands are recorded.
		/// </summary>
		public Log CommandsLog {
			get { return commands_log; }
		}

		/// <summary>
		/// Returns the conglomerate for this database.
		/// </summary>
		internal TableDataConglomerate Conglomerate {
			get { return conglomerate; }
		}

		/// <summary>
		/// Gets <b>true</b> if the database exists.
		/// </summary>
		/// <remarks>
		/// The test should be called before <see cref="Init"/> method to check
		/// if the database already exists.
		/// </remarks>
		public bool Exists {
			get {
				if (initialised) {
					throw new Exception(
						"The database is initialised, so no point testing it's existance.");
				}

				try {
					// HACK: If the legacy style '.sf' state file exists then we must return
					//   true here because technically the database exists but is not in the
					//   correct version.
					if (conglomerate.Exists(Name)) {
						return true;
					} else {
						bool is_file_s_system =
							(system.StoreSystem is V1FileStoreSystem);
						if (is_file_s_system &&
						    File.Exists(Path.Combine(system.DatabasePath, Name + ".sf"))) {
							return true;
						}
					}
					return false;
				} catch (IOException e) {
					Debug.WriteException(e);
					throw new Exception("IO Error: " + e.Message);
				}
			}
		}

		/// <summary>
		/// Returns true if the database is initialised.
		/// </summary>
		public bool IsInitialized {
			get { return initialised; }
		}

		/// <summary>
		/// Returns the <see cref="DatabaseSystem"/> that this Database is from.
		/// </summary>
		public DatabaseSystem System {
			get { return system; }
		}

		/// <summary>
		/// Returns the IStoreSystem for this Database.
		/// </summary>
		internal IStoreSystem StoreSystem {
			get { return system.StoreSystem; }
		}

		/// <summary>
		/// Convenience static for accessing the global Stats object.
		/// </summary>
		// Perhaps this should be deprecated?
		public Stats Stats {
			get { return System.Stats; }
		}

		/// <summary>
		/// Returns the system trigger manager.
		/// </summary>
		internal TriggerManager TriggerManager {
			get { return trigger_manager; }
		}

		/// <summary>
		/// Returns the system user manager.
		/// </summary>
		public UserManager UserManager {
			get { return System.UserManager; }
		}

		/// <summary>
		/// Returns the system DataCellCache.
		/// </summary>
		internal DataCellCache DataCellCache {
			get { return System.DataCellCache; }
		}

		/// <summary>
		/// Returns true if the database has shut down.
		/// </summary>
		public bool HasShutDown {
			get { return System.HasShutDown; }
		}

		/// <summary>
		/// Returns a static table that has a single row but no columns.
		/// </summary>
		/// <remarks>
		/// This table is useful for certain database operations.
		/// </remarks>
		public Table SingleRowTable {
			get { return single_row_table; }
		}

		/// <summary>
		/// Gets the <see cref="IDebugLogger"/> implementation from the parent 
		/// <see cref="DatabaseSystem"/> context.
		/// </summary>
		public IDebugLogger Debug {
			get { return System.Debug; }
		}

		/// <summary>
		/// Returns a new <see cref="DatabaseConnection"/> instance that is 
		/// used against this database.
		/// </summary>
		/// <param name="user"></param>
		/// <param name="call_back"></param>
		/// <remarks>
		/// When a new connection is made on this database, this method is 
		/// called to create a new <see cref="DatabaseConnection"/> instance 
		/// for the connection. This connection handles all transactional 
		/// queries and modifications to the database.
		/// </remarks>
		/// <returns></returns>
		public DatabaseConnection CreateNewConnection(User user, DatabaseConnection.CallBack call_back) {
			if (user == null)
				user = InternalSystemUser;

			DatabaseConnection connection = new DatabaseConnection(this, user, call_back);
			// Initialize the connection
			connection.Init();

			return connection;
		}

		// ---------- Database user management functions ----------

		/// <summary>
		/// Tries to authenticate a username/password against this database.
		/// </summary>
		/// <remarks>
		/// If a valid object is returned, the user will be logged into 
		/// the engine via the <see cref="Data.UserManager"/>. The developer must 
		/// ensure that <see cref="Dispose()"/> is called before the object is 
		/// disposed (logs out of the system).
		/// <para>
		/// This method also returns <b>null</b> if a user exists but was 
		/// denied access from the given host string. The given <i>host name</i>
		/// is formatted in the database host connection encoding. This 
		/// method checks all the values from the <see cref="SysUserpriv"/> 
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
		public User AuthenticateUser(String username, String password, String connection_string) {
			// Create a temporary connection for authentication only...
			DatabaseConnection connection = CreateNewConnection(null, null);
			DatabaseQueryContext context = new DatabaseQueryContext(connection);
			connection.CurrentSchema = SystemSchema;
			LockingMechanism locker = connection.LockingMechanism;
			locker.SetMode(LockingMode.Exclusive);
			try {
				try {
					IDbConnection conn = connection.GetDbConnection();

					// Is the username/password in the database?
					IDbCommand command = conn.CreateCommand();
					command.CommandText = " SELECT \"UserName\" FROM \"sUSRPassword\" " +
					                      "  WHERE \"sUSRPassword.UserName\" = ? " +
					                      "    AND \"sUSRPassword.Password\" = ? ";
					command.Parameters.Add(username);
					command.Parameters.Add(password);
					command.Prepare();

					IDataReader rs = command.ExecuteReader();
					if (!rs.Read())
						return null;
					rs.Close();

					// Now check if this user is permitted to connect from the given
					// host.
					if (UserAllowedAccessFromHost(context,
					                              username, connection_string)) {
						// Successfully authenticated...
						User user = new User(username, this,
						                    connection_string, DateTime.Now);
						// Log the authenticated user in to the engine.
						system.UserManager.OnUserLoggedIn(user);
						return user;
					}

					return null;
				} catch (DataException e) {
					if (e is DbDataException) {
						DbDataException dbDataException = (DbDataException)e;
						Debug.Write(DebugLevel.Error, this, dbDataException.ServerErrorStackTrace);
					}
					Debug.WriteException(DebugLevel.Error, e);
					throw new Exception("SQL Error: " + e.Message);
				}
			} finally {
				try {
					// Make sure we commit the connection.
					connection.Commit();
				} catch (TransactionException e) {
					// Just issue a warning...
					Debug.WriteException(DebugLevel.Warning, e);
				} finally {
					// Guarentee that we unluck from EXCLUSIVE
					locker.FinishMode(LockingMode.Exclusive);
				}
				// And make sure we close (dispose) of the temporary connection.
				connection.Close();
			}
		}

		/// <summary>
		/// Performs check to determine if user is allowed access from the given
		/// host.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="username">The name of the user to check the host for.</param>
		/// <param name="connection_string">The full connection string.</param>
		/// <returns>
		/// Returns <b>true</b> if the user identified by the given <paramref name="username"/>
		/// is allowed to access for the host specified in the <paramref name="connection_string"/>,
		/// otherwise <b>false</b>.
		/// </returns>
		private bool UserAllowedAccessFromHost(DatabaseQueryContext context, String username, String connection_string) {
			// The system user is not allowed to login
			if (username.Equals(InternalSecureUsername)) {
				return false;
			}

			// We always allow access from 'Internal/*' (connections from the
			// 'GetConnection' method of a com.mckoi.database.control.DbSystem object)
			// ISSUE: Should we add this as a rule?
			if (connection_string.StartsWith("Internal/")) {
				return true;
			}

			// What's the protocol?
			int protocol_host_deliminator = connection_string.IndexOf("/");
			String protocol =
				connection_string.Substring(0, protocol_host_deliminator);
			String host = connection_string.Substring(protocol_host_deliminator + 1);

			if (Debug.IsInterestedIn(DebugLevel.Information)) {
				Debug.Write(DebugLevel.Information, this,
				            "Checking host: protocol = " + protocol +
				            ", host = " + host);
			}

			// The table to check
			DataTable connect_priv = context.GetTable(SysUserconnect);
			VariableName un_col = connect_priv.GetResolvedVariable(0);
			VariableName proto_col = connect_priv.GetResolvedVariable(1);
			VariableName host_col = connect_priv.GetResolvedVariable(2);
			VariableName access_col = connect_priv.GetResolvedVariable(3);
			// Query: where UserName = %username%
			Table t = connect_priv.SimpleSelect(context, un_col, Operator.Get("="),
			                                    new Expression(TObject.GetString(username)));
			// Query: where %protocol% like Protocol
			Expression exp = Expression.Simple(TObject.GetString(protocol),
			                                   Operator.Get("like"), proto_col);
			t = t.ExhaustiveSelect(context, exp);
			// Query: where %host% like Host
			exp = Expression.Simple(TObject.GetString(host),
			                        Operator.Get("like"), host_col);
			t = t.ExhaustiveSelect(context, exp);

			// Those that are DENY
			Table t2 = t.SimpleSelect(context, access_col, Operator.Get("="),
			                          new Expression(TObject.GetString("DENY")));
			if (t2.RowCount > 0) {
				return false;
			}
			// Those that are ALLOW
			Table t3 = t.SimpleSelect(context, access_col, Operator.Get("="),
			                          new Expression(TObject.GetString("ALLOW")));
			if (t3.RowCount > 0) {
				return true;
			}
			// No DENY or ALLOW entries for this host so deny access.
			return false;
		}

		/// <summary>
		/// Checks if a user exists within the database.
		/// </summary>
		/// <param name="context">The context of the session.</param>
		/// <param name="username">The name of the user to check.</param>
		/// <remarks>
		/// <b>Note:</b> Assumes exclusive Lock on the session.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if the user identified by the given 
		/// <paramref name="username"/>, otherwise <b>false</b>.
		/// </returns>
		public bool UserExists(DatabaseQueryContext context, String username) {
			DataTable table = context.GetTable(SysPassword);
			VariableName c1 = table.GetResolvedVariable(0);
			// All sUSRPassword where UserName = %username%
			Table t = table.SimpleSelect(context, c1, Operator.Get("="), new Expression(TObject.GetString(username)));
			return t.RowCount > 0;
		}

		/// <summary>
		/// Creates a new user for the database.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="username">The name of the user to create.</param>
		/// <param name="password">The user password.</param>
		/// <remarks>
		/// <b>Note</b>: Assumes exclusive Lock on <see cref="DatabaseConnection"/>.
		/// </remarks>
		/// <exception cref="DatabaseException">
		/// If the user is already defined by the database
		/// </exception>
		public void CreateUser(DatabaseQueryContext context, String username, String password) {
			if (username == null || password == null) {
				throw new DatabaseException("Username or password can not be NULL.");
			}

			// The username must be more than 1 character
			if (username.Length <= 1) {
				throw new DatabaseException("Username must be at least 2 characters.");
			}

			// The password must be more than 1 character
			if (password.Length <= 1) {
				throw new DatabaseException("Password must be at least 2 characters.");
			}

			// Check the user doesn't already exist
			if (UserExists(context, username)) {
				throw new DatabaseException("User '" + username + "' already exists.");
			}

			// Some usernames are reserved words
			if (String.Compare(username, "public", true) == 0) {
				throw new DatabaseException("User '" + username +
				                            "' not allowed - reserved.");
			}

			// Usernames starting with @, &, # and $ are reserved for system
			// identifiers
			char c = username[0];
			if (c == '@' || c == '&' || c == '#' || c == '$') {
				throw new DatabaseException("User name can not start with '" + c +
				                            "' character.");
			}

			// Add this user to the password table.
			DataTable table = context.GetTable(SysPassword);
			RowData rdat = new RowData(table);
			rdat.SetColumnDataFromObject(0, username);
			rdat.SetColumnDataFromObject(1, password);
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
		public void DeleteAllUserGroups(DatabaseQueryContext context, String username) {
			Operator EQUALS_OP = Operator.Get("=");
			Expression USER_EXPR = new Expression(TObject.GetString(username));

			DataTable table = context.GetTable(SysUserpriv);
			VariableName c1 = table.GetResolvedVariable(0);
			// All sUSRUserPriv where UserName = %username%
			Table t = table.SimpleSelect(context, c1, EQUALS_OP, USER_EXPR);
			// Delete all the groups
			table.Delete(t);
		}

		/// <summary>
		/// Drops a user from the database.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="username">The name of the user to drop.</param>
		/// <remarks>
		/// This also deletes all information associated with a user such as 
		/// the groups they belong to. It does not delete the privs a user 
		/// has set up.
		/// <para>
		/// <b>Note:</b> Assumes exclusive Lock on database session.
		/// </para>
		/// </remarks>
		public void DeleteUser(DatabaseQueryContext context, String username) {
			// PENDING: This should check if there are any tables the user has setup
			//  and not allow the delete if there are.

			Operator EQUALS_OP = Operator.Get("=");
			Expression USER_EXPR = new Expression(TObject.GetString(username));

			// First delete all the groups from the user priv table
			DeleteAllUserGroups(context, username);

			// Now delete the username from the sUSRUserConnectPriv table
			DataTable table = context.GetTable(SysUserconnect);
			VariableName c1 = table.GetResolvedVariable(0);
			Table t = table.SimpleSelect(context, c1, EQUALS_OP, USER_EXPR);
			table.Delete(t);

			// Finally delete the username from the sUSRPassword table
			table = context.GetTable(SysPassword);
			c1 = table.GetResolvedVariable(0);
			t = table.SimpleSelect(context, c1, EQUALS_OP, USER_EXPR);
			table.Delete(t);
		}

		/// <summary>
		/// Alters the password of the given user.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="username">The name of the user to alter the password.</param>
		/// <param name="password">The new password for the user.</param>
		/// <remarks>
		/// <b>Note:</b> Assumes exclusive Lock on database session.
		/// </remarks>
		public void AlterUserPassword(DatabaseQueryContext context, String username, String password) {
			Operator EQUALS_OP = Operator.Get("=");
			Expression USER_EXPR = new Expression(TObject.GetString(username));

			// Delete the current username from the sUSRPassword table
			DataTable table = context.GetTable(SysPassword);
			VariableName c1 = table.GetResolvedVariable(0);
			Table t = table.SimpleSelect(context, c1, EQUALS_OP, USER_EXPR);
			if (t.RowCount == 1) {
				table.Delete(t);

				// Add the new username
				table = context.GetTable(SysPassword);
				RowData rdat = new RowData(table);
				rdat.SetColumnDataFromObject(0, username);
				rdat.SetColumnDataFromObject(1, password);
				table.Add(rdat);
			} else {
				throw new DatabaseException("Username '" + username + "' was not found.");
			}
		}

		/// <summary>
		/// Returns the list of all user groups the user belongs to.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="username"></param>
		/// <returns></returns>
		public String[] GroupsUserBelongsTo(DatabaseQueryContext context, String username) {
			DataTable table = context.GetTable(SysUserpriv);
			VariableName c1 = table.GetResolvedVariable(0);
			// All sUSRUserPriv where UserName = %username%
			Table t = table.SimpleSelect(context, c1, Operator.Get("="),
			                             new Expression(TObject.GetString(username)));
			int sz = t.RowCount;
			string[] groups = new string[sz];
			IRowEnumerator row_enum = t.GetRowEnumerator();
			int i = 0;
			while (row_enum.MoveNext()) {
				groups[i] = t.GetCellContents(1, row_enum.RowIndex).Object.ToString();
				++i;
			}

			return groups;
		}

		/// <summary>
		/// Checks if a user belongs in a specified group.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="username">The name of the user to check.</param>
		/// <param name="group">The name of the group to check.</param>
		/// <remarks>
		/// <b>Note</b> Assumes exclusive Lock on database session.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if the given user belongs to the given
		/// <paramref name="group"/>, otherwise <b>false</b>.
		/// </returns>
		public bool UserBelongsToGroup(DatabaseQueryContext context,
		                               String username, String group) {
			DataTable table = context.GetTable(SysUserpriv);
			VariableName c1 = table.GetResolvedVariable(0);
			VariableName c2 = table.GetResolvedVariable(1);
			// All sUSRUserPriv where UserName = %username%
			Table t = table.SimpleSelect(context, c1, Operator.Get("="),
			                             new Expression(TObject.GetString(username)));
			// All from this set where PrivGroupName = %group%
			t = t.SimpleSelect(context, c2, Operator.Get("="),
			                   new Expression(TObject.GetString(group)));
			return t.RowCount > 0;
		}

		/// <summary>
		/// Adds a user to the given group.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="username">The name of the user to be added.</param>
		/// <param name="group">The name of the group to add the user to.</param>
		/// <remarks>
		/// This makes an entry in the <see cref="SysUserpriv"/> for this user 
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
		public void AddUserToGroup(DatabaseQueryContext context,
		                           String username, String group) {
			if (group == null) {
				throw new DatabaseException("Can add NULL group.");
			}

			// Groups starting with @, &, # and $ are reserved for system
			// identifiers
			char c = group[0];
			if (c == '@' || c == '&' || c == '#' || c == '$') {
				throw new DatabaseException("The group name can not start with '" + c +
				                            "' character.");
			}

			// Check the user doesn't belong to the group
			if (!UserBelongsToGroup(context, username, group)) {
				// The user priv table
				DataTable table = context.GetTable(SysUserpriv);
				// Add this user to the group.
				RowData rdat = new RowData(table);
				rdat.SetColumnDataFromObject(0, username);
				rdat.SetColumnDataFromObject(1, group);
				table.Add(rdat);
			}
			// NOTE: we silently ignore the case when a user already belongs to the
			//   group.
		}

		/// <summary>
		/// Sets the Lock status for the given user.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="user">The user to set the Lock status.</param>
		/// <param name="lock_status"></param>
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
		public void SetUserLock(DatabaseQueryContext context, User user, bool lock_status) {
			String username = user.UserName;

			// Internally we implement this by adding the user to the #locked group.
			DataTable table = context.GetTable(SysUserpriv);
			VariableName c1 = table.GetResolvedVariable(0);
			VariableName c2 = table.GetResolvedVariable(1);
			// All sUSRUserPriv where UserName = %username%
			Table t = table.SimpleSelect(context, c1, Operator.Get("="),
			                             new Expression(TObject.GetString(username)));
			// All from this set where PrivGroupName = %group%
			t = t.SimpleSelect(context, c2, Operator.Get("="),
			                   new Expression(TObject.GetString(LockGroup)));

			bool user_belongs_to_lock_group = t.RowCount > 0;
			if (lock_status && !user_belongs_to_lock_group) {
				// Lock the user by adding the user to the Lock group
				// Add this user to the locked group.
				RowData rdat = new RowData(table);
				rdat.SetColumnDataFromObject(0, username);
				rdat.SetColumnDataFromObject(1, LockGroup);
				table.Add(rdat);
			} else if (!lock_status && user_belongs_to_lock_group) {
				// Unlock the user by removing the user from the Lock group
				// Remove this user from the locked group.
				table.Delete(t);
			}
		}

		/// <summary>
		/// Grants the given user access to connect to the database from the
		/// given host address.
		/// </summary>
		/// <param name="context"></param>
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
		public void GrantHostAccessToUser(DatabaseQueryContext context,
		                                  String user, String protocol, String host) {
			// The user connect priv table
			DataTable table = context.GetTable(SysUserconnect);
			// Add the protocol and host to the table
			RowData rdat = new RowData(table);
			rdat.SetColumnDataFromObject(0, user);
			rdat.SetColumnDataFromObject(1, protocol);
			rdat.SetColumnDataFromObject(2, host);
			rdat.SetColumnDataFromObject(3, "ALLOW");
			table.Add(rdat);
		}

		/// <summary>
		/// Checks if the given user belongs to secure group.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="user"></param>
		/// <returns>
		/// Returns <b>true</b> if the user belongs to the secure access
		/// privileges group, otherwise <b>false</b>.
		/// </returns>
		private bool UserHasSecureAccess(DatabaseQueryContext context, User user) {
			// The internal secure user has full privs on everything
			if (user.UserName.Equals(InternalSecureUsername)) {
				return true;
			}
			return UserBelongsToGroup(context, user.UserName, SecureGroup);
		}

		/// <summary>
		/// Checks if the given user is permitted the given grant for
		/// executing operations on the given schema.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="user"></param>
		/// <param name="schema"></param>
		/// <param name="grant"></param>
		/// <returns>
		/// Returns <b>true</b> if the grant manager permits a schema 
		/// operation (eg, <i>CREATE</i>, <i>ALTER</i> and <i>DROP</i> 
		/// table operations) for the given user, otherwise <b>false</b>.
		/// </returns>
		private static bool UserHasSchemaGrant(DatabaseQueryContext context,
		                                       User user, String schema, int grant) {
			// The internal secure user has full privs on everything
			if (user.UserName.Equals(InternalSecureUsername)) {
				return true;
			}

			// No users have schema access to the system schema.
			if (schema.Equals(SystemSchema)) {
				return false;
			}

			// Ask the grant manager if there are any privs setup for this user on the
			// given schema.
			GrantManager manager = context.GrantManager;
			Privileges privs = manager.GetUserGrants(
				GrantObject.Schema, schema, user.UserName);

			return privs.Permits(grant);
		}

		/// <summary>
		/// Checks if the given user is permitted the given grant for
		/// executing operations on the given table.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="user"></param>
		/// <param name="table_name"></param>
		/// <param name="columns"></param>
		/// <param name="grant"></param>
		/// <returns>
		/// Returns <b>true</b> if the grant manager permits a schema 
		/// operation (eg, <c>CREATE</c>, <c>ALTER</c> and <c>DROP</c> 
		/// table operations) for the given user, otherwise <b>false</b>.
		/// </returns>
		private static bool UserHasTableObjectGrant(DatabaseQueryContext context,
		                                            User user, TableName table_name, VariableName[] columns,
		                                            int grant) {
			// The internal secure user has full privs on everything
			if (user.UserName.Equals(InternalSecureUsername)) {
				return true;
			}

			// PENDING: Support column level privileges.

			// Ask the grant manager if there are any privs setup for this user on the
			// given schema.
			GrantManager manager = context.GrantManager;
			Privileges privs = manager.GetUserGrants(
				GrantObject.Table, table_name.ToString(), user.UserName);

			return privs.Permits(grant);
		}

		/// <summary>
		/// Checks if the given user can create users on the database.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="user"></param>
		/// <remarks>
		/// Only members of the <i>secure access</i> group, or the 
		/// <i>user manager</i> group can do this.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if the user is permitted to create, 
		/// alter and drop user information from the database, otherwise 
		/// returns <b>false</b>.
		/// </returns>
		public bool CanUserCreateAndDropUsers(
			DatabaseQueryContext context, User user) {
			return (UserHasSecureAccess(context, user) ||
			        UserBelongsToGroup(context, user.UserName,
			                           UserManagerGroup));
		}

		/// <summary>
		/// Returns true if the user is permitted to create and drop schema's in the
		/// database, otherwise returns false.
		/// </summary>
		/// <remarks>
		/// Only members of the 'secure access' group, or the 'schema manager' group 
		/// can do this.
		/// </remarks>
		public bool CanUserCreateAndDropSchema(
			DatabaseQueryContext context, User user, String schema) {
			// The internal secure user has full privs on everything
			if (user.UserName.Equals(InternalSecureUsername)) {
				return true;
			}

			// No user can create or drop the system schema.
			if (schema.Equals(SystemSchema)) {
				return false;
			} else {
				return (UserHasSecureAccess(context, user) ||
				        UserBelongsToGroup(context, user.UserName,
				                           SchemaManagerGroup));
			}
		}

		/// <summary>
		/// Returns true if the user can shut down the database server.
		/// </summary>
		/// <remarks>
		/// A user can shut down the database if they are a member of the 
		/// 'secure acces' group.
		/// </remarks>
		public bool CanUserShutDown(DatabaseQueryContext context, User user) {
			return UserHasSecureAccess(context, user);
		}

		/// <summary>
		/// Returns true if the user is allowed to execute the given stored procedure.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="user"></param>
		/// <param name="procedure_name"></param>
		/// <returns></returns>
		public bool CanUserExecuteStoredProcedure(DatabaseQueryContext context,
		                                          User user, String procedure_name) {
			// Currently you can only execute a procedure if you are a member of the
			// secure access priv group.
			return UserHasSecureAccess(context, user);
		}

		// ---- General schema level privs ----

		/// <summary>
		/// Returns true if the user can create a table or view with the given name,
		/// otherwise returns false.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="user"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		public bool CanUserCreateTableObject(DatabaseQueryContext context, User user, TableName table) {
			if (UserHasSchemaGrant(context, user, table.Schema, Privileges.Create)) {
				return true;
			}

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context, user);
		}

		/// <summary>
		/// Returns true if the user can alter a table or view with the given name,
		/// otherwise returns false.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="user"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		public bool CanUserAlterTableObject(DatabaseQueryContext context, User user, TableName table) {
			if (UserHasSchemaGrant(context, user, table.Schema, Privileges.Alter))
				return true;

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context, user);
		}

		/// <summary>
		/// Returns true if the user can drop a table or view with the given name,
		/// otherwise returns false.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="user"></param>
		/// <param name="table"></param>
		/// <returns></returns>
		public bool CanUserDropTableObject(DatabaseQueryContext context, User user, TableName table) {
			if (UserHasSchemaGrant(context, user, table.Schema, Privileges.Drop)) {
				return true;
			}

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context, user);
		}

		// ---- Check table object privs ----

		/// <summary>
		/// Returns true if the user can select from a table or view with the given
		/// name and given columns, otherwise returns false.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="user"></param>
		/// <param name="table"></param>
		/// <param name="columns"></param>
		/// <returns></returns>
		public bool CanUserSelectFromTableObject(
			DatabaseQueryContext context, User user, TableName table,
			VariableName[] columns) {
			if (UserHasTableObjectGrant(context, user, table, columns,
			                            Privileges.Select)) {
				return true;
			}

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context, user);
		}

		/// <summary>
		///  Returns true if the user can insert into a table or view with the given
		/// name and given columns, otherwise returns false.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="user"></param>
		/// <param name="table"></param>
		/// <param name="columns"></param>
		/// <returns></returns>
		public bool CanUserInsertIntoTableObject(
			DatabaseQueryContext context, User user, TableName table,
			VariableName[] columns) {
			if (UserHasTableObjectGrant(context, user, table, columns,
			                            Privileges.Insert)) {
				return true;
			}

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context, user);
		}

		/// <summary>
		/// Returns true if the user can update a table or view with the given
		/// name and given columns, otherwise returns false.
		/// </summary>
		public bool CanUserUpdateTableObject(
			DatabaseQueryContext context, User user, TableName table,
			VariableName[] columns) {
			if (UserHasTableObjectGrant(context, user, table, columns,
			                            Privileges.Update)) {
				return true;
			}

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context, user);
		}

		///<summary>
		/// Returns true if the user can delete from a table or view with the given
		/// name and given columns, otherwise returns false.
		///</summary>
		///<param name="context"></param>
		///<param name="user"></param>
		///<param name="table"></param>
		///<returns></returns>
		public bool CanUserDeleteFromTableObject(
			DatabaseQueryContext context, User user, TableName table) {
			if (UserHasTableObjectGrant(context, user, table, null,
			                            Privileges.Delete)) {
				return true;
			}

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context, user);
		}

		///<summary>
		/// Returns true if the user can compact a table with the given name,
		/// otherwise returns false.
		///</summary>
		///<param name="context"></param>
		///<param name="user"></param>
		///<param name="table"></param>
		///<returns></returns>
		public bool CanUserCompactTableObject(
			DatabaseQueryContext context, User user, TableName table) {
			if (UserHasTableObjectGrant(context, user, table, null,
			                            Privileges.Compact)) {
				return true;
			}

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context, user);
		}

		///<summary>
		/// Returns true if the user can create a procedure with the given name,
		/// otherwise returns false.
		///</summary>
		///<param name="context"></param>
		///<param name="user"></param>
		///<param name="table"></param>
		///<returns></returns>
		public bool CanUserCreateProcedureObject(
			DatabaseQueryContext context, User user, TableName table) {
			if (UserHasSchemaGrant(context, user,
			                       table.Schema, Privileges.Create)) {
				return true;
			}

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context, user);
		}

		///<summary>
		/// Returns true if the user can drop a procedure with the given name,
		/// otherwise returns false.
		///</summary>
		///<param name="context"></param>
		///<param name="user"></param>
		///<param name="table"></param>
		///<returns></returns>
		public bool CanUserDropProcedureObject(
			DatabaseQueryContext context, User user, TableName table) {
			if (UserHasSchemaGrant(context, user,
			                       table.Schema, Privileges.Drop)) {
				return true;
			}

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context, user);
		}

		///<summary>
		///  Returns true if the user can create a sequence with the given name,
		/// otherwise returns false.
		///</summary>
		///<param name="context"></param>
		///<param name="user"></param>
		///<param name="table"></param>
		///<returns></returns>
		public bool CanUserCreateSequenceObject(
			DatabaseQueryContext context, User user, TableName table) {
			if (UserHasSchemaGrant(context, user,
			                       table.Schema, Privileges.Create)) {
				return true;
			}

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context, user);
		}

		///<summary>
		/// Returns true if the user can drop a sequence with the given name,
		/// otherwise returns false.
		///</summary>
		///<param name="context"></param>
		///<param name="user"></param>
		///<param name="table"></param>
		///<returns></returns>
		public bool CanUserDropSequenceObject(
			DatabaseQueryContext context, User user, TableName table) {
			if (UserHasSchemaGrant(context, user,
			                       table.Schema, Privileges.Drop)) {
				return true;
			}

			// If the user belongs to the secure access priv group, return true
			return UserHasSecureAccess(context, user);
		}


		// ---------- Schema management ----------

		private static void CreateSchemaInfoTables(DatabaseConnection connection) {
			connection.CreateSchema(DefaultSchema, "DEFAULT");
			connection.CreateSchema(InformationSchema, "SYSTEM");
		}

		/// <summary>
		///  Creates all the system views.
		/// </summary>
		/// <param name="connection"></param>
		private void CreateSystemViews(DatabaseConnection connection) {
			// Obtain the data interface.
			try {
				IDbConnection db_conn = connection.GetDbConnection();

				// Is the username/password in the database?
				IDbCommand stmt = db_conn.CreateCommand();

				// This view shows the grants that the user has (no join, only priv_bit).
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserSimpleGrant AS " +
					"  SELECT \"priv_bit\", \"object\", \"param\", \"grantee\", " +
					"         \"grant_option\", \"granter\" " +
					"    FROM SYSTEM.sUSRGrant " +
					"   WHERE ( grantee = user() OR grantee = '@PUBLIC' )";
				stmt.ExecuteNonQuery();

				// This view shows the grants that the user is allowed to see
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserGrant AS " +
					"  SELECT \"description\", \"object\", \"param\", \"grantee\", " +
					"         \"grant_option\", \"granter\" " +
					"    FROM SYSTEM.sUSRGrant, SYSTEM.sUSRPrivMap " +
					"   WHERE ( grantee = user() OR grantee = '@PUBLIC' )" +
					"     AND sUSRGrant.priv_bit = sUSRPrivMap.priv_bit";
				stmt.ExecuteNonQuery();

				// A view that represents the list of schema this user is allowed to view
				// the contents of.
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserSchemaInfo AS " +
					"  SELECT * FROM SYSTEM.sUSRSchemaInfo " +
					"   WHERE \"name\" IN ( " +
					"     SELECT \"param\" " +
					"       FROM INFORMATION_SCHEMA.ThisUserGrant " +
					"      WHERE \"object\" = 65 " +
					"        AND \"description\" = 'LIST' )";
				stmt.ExecuteNonQuery();

				// A view that exposes the sUSRTableColumn table but only for the tables
				// this user has Read access to.
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserTableColumns AS " +
					"  SELECT * FROM SYSTEM.sUSRTableColumns " +
					"   WHERE \"schema\" IN ( " +
					"     SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )";
				stmt.ExecuteNonQuery();

				// A view that exposes the sUSRTableInfo table but only for the tables
				// this user has Read access to.
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserTableInfo AS " +
					"  SELECT * FROM SYSTEM.sUSRTableInfo " +
					"   WHERE \"schema\" IN ( " +
					"     SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.TABLES AS " +
					"  SELECT NULL AS \"TABLE_CATALOG\", \n" +
					"         \"schema\" AS \"TABLE_SCHEMA\", \n" +
					"         \"name\" AS \"TABLE_NAME\", \n" +
					"         \"type\" AS \"TABLE_TYPE\", \n" +
					"         \"other\" AS \"REMARKS\", \n" +
					"         NULL AS \"TYPE_CATALOG\", \n" +
					"         NULL AS \"TYPE_SCHEMA\", \n" +
					"         NULL AS \"TYPE_NAME\", \n" +
					"         NULL AS \"SELF_REFERENCING_COL_NAME\", \n" +
					"         NULL AS \"REF_GENERATION\" \n" +
					"    FROM INFORMATION_SCHEMA.ThisUserTableInfo \n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.SCHEMATA AS " +
					"  SELECT \"name\" AS \"TABLE_SCHEMA\", \n" +
					"         NULL AS \"TABLE_CATALOG\" \n" +
					"    FROM INFORMATION_SCHEMA.ThisUserSchemaInfo\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.CATALOGS AS " +
					"  SELECT NULL AS \"TABLE_CATALOG\" \n" +
					"    FROM SYSTEM.sUSRSchemaInfo\n" + // Hacky, this will generate a 0 row
					"   WHERE FALSE\n"; // table.
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.COLUMNS AS " +
					"  SELECT NULL AS \"TABLE_CATALOG\",\n" +
					"         \"schema\" AS \"TABLE_SCHEMA\",\n" +
					"         \"table\" AS \"TABLE_NAME\",\n" +
					"         \"column\" AS \"COLUMN_NAME\",\n" +
					"         \"sql_type\" AS \"DATA_TYPE\",\n" +
					"         \"type_desc\" AS \"TYPE_NAME\",\n" +
					"         IF(\"size\" = -1, 1024, \"size\") AS \"COLUMN_SIZE\",\n" +
					"         NULL AS \"BUFFER_LENGTH\",\n" +
					"         \"scale\" AS \"DECIMAL_DIGITS\",\n" +
					"         IF(\"sql_type\" = -7, 2, 10) AS \"NUM_PREC_RADIX\",\n" +
					"         IF(\"not_null\", 0, 1) AS \"NULLABLE\",\n" +
					"         '' AS \"REMARKS\",\n" +
					"         \"default\" AS \"COLUMN_DEFAULT\",\n" +
					"         NULL AS \"SQL_DATA_TYPE\",\n" +
					"         NULL AS \"SQL_DATETIME_SUB\",\n" +
					"         IF(\"size\" = -1, 1024, \"size\") AS \"CHAR_OCTET_LENGTH\",\n" +
					"         \"seq_no\" + 1 AS \"ORDINAL_POSITION\",\n" +
					"         IF(\"not_null\", 'NO', 'YES') AS \"IS_NULLABLE\"\n" +
					"    FROM INFORMATION_SCHEMA.ThisUserTableColumns\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.COLUMN_PRIVILEGES AS " +
					"  SELECT \"TABLE_CATALOG\",\n" +
					"         \"TABLE_SCHEMA\",\n" +
					"         \"TABLE_NAME\",\n" +
					"         \"COLUMN_NAME\",\n" +
					"         IF(\"ThisUserGrant.granter\" = '@SYSTEM', \n" +
					"                        NULL, \"ThisUserGrant.granter\") AS \"GRANTOR\",\n" +
					"         IF(\"ThisUserGrant.grantee\" = '@PUBLIC', \n" +
					"                    'public', \"ThisUserGrant.grantee\") AS \"GRANTEE\",\n" +
					"         \"ThisUserGrant.description\" AS \"PRIVILEGE\",\n" +
					"         IF(\"grant_option\" = 'true', 'YES', 'NO') AS \"IS_GRANTABLE\" \n" +
					"    FROM INFORMATION_SCHEMA.COLUMNS, INFORMATION_SCHEMA.ThisUserGrant \n" +
					"   WHERE CONCAT(COLUMNS.TABLE_SCHEMA, '.', COLUMNS.TABLE_NAME) = \n" +
					"         ThisUserGrant.param \n" +
					"     AND INFORMATION_SCHEMA.ThisUserGrant.object = 1 \n" +
					"     AND INFORMATION_SCHEMA.ThisUserGrant.description IS NOT NULL \n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.TABLE_PRIVILEGES AS " +
					"  SELECT \"TABLE_CATALOG\",\n" +
					"         \"TABLE_SCHEMA\",\n" +
					"         \"TABLE_NAME\",\n" +
					"         IF(\"ThisUserGrant.granter\" = '@SYSTEM', \n" +
					"                        NULL, \"ThisUserGrant.granter\") AS \"GRANTOR\",\n" +
					"         IF(\"ThisUserGrant.grantee\" = '@PUBLIC', \n" +
					"                    'public', \"ThisUserGrant.grantee\") AS \"GRANTEE\",\n" +
					"         \"ThisUserGrant.description\" AS \"PRIVILEGE\",\n" +
					"         IF(\"grant_option\" = 'true', 'YES', 'NO') AS \"IS_GRANTABLE\" \n" +
					"    FROM INFORMATION_SCHEMA.TABLES, INFORMATION_SCHEMA.ThisUserGrant \n" +
					"   WHERE CONCAT(TABLES.TABLE_SCHEMA, '.', TABLES.TABLE_NAME) = \n" +
					"         ThisUserGrant.param \n" +
					"     AND INFORMATION_SCHEMA.ThisUserGrant.object = 1 \n" +
					"     AND INFORMATION_SCHEMA.ThisUserGrant.description IS NOT NULL \n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.PrimaryKeys AS " +
					"  SELECT NULL \"TABLE_CATALOG\",\n" +
					"         \"schema\" \"TABLE_SCHEMA\",\n" +
					"         \"table\" \"TABLE_NAME\",\n" +
					"         \"column\" \"COLUMN_NAME\",\n" +
					"         \"SYSTEM.sUSRPrimaryColumns.seq_no\" \"KEY_SEQ\",\n" +
					"         \"name\" \"PK_NAME\"\n" +
					"    FROM SYSTEM.sUSRPKeyInfo, SYSTEM.sUSRPrimaryColumns\n" +
					"   WHERE sUSRPKeyInfo.id = sUSRPrimaryColumns.pk_id\n" +
					"     AND \"schema\" IN\n" +
					"            ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.ImportedKeys AS " +
					"  SELECT NULL \"PKTABLE_CATALOG\",\n" +
					"         \"sUSRFKeyInfo.ref_schema\" \"PKTABLE_SCHEMA\",\n" +
					"         \"sUSRFKeyInfo.ref_table\" \"PKTABLE_NAME\",\n" +
					"         \"sUSRForeignColumns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
					"         NULL \"FKTABLE_CATALOG\",\n" +
					"         \"sUSRFKeyInfo.schema\" \"FKTABLE_SCHEMA\",\n" +
					"         \"sUSRFKeyInfo.table\" \"FKTABLE_NAME\",\n" +
					"         \"sUSRForeignColumns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
					"         \"sUSRForeignColumns.seq_no\" \"KEY_SEQ\",\n" +
					"         I_FRULE_CONVERT(\"sUSRFKeyInfo.update_rule\") \"UPDATE_RULE\",\n" +
					"         I_FRULE_CONVERT(\"sUSRFKeyInfo.delete_rule\") \"DELETE_RULE\",\n" +
					"         \"sUSRFKeyInfo.name\" \"FK_NAME\",\n" +
					"         NULL \"PK_NAME\",\n" +
					"         \"sUSRFKeyInfo.deferred\" \"DEFERRABILITY\"\n" +
					"    FROM SYSTEM.sUSRFKeyInfo, SYSTEM.sUSRForeignColumns\n" +
					"   WHERE sUSRFKeyInfo.id = sUSRForeignColumns.fk_id\n" +
					"     AND \"sUSRFKeyInfo.schema\" IN\n" +
					"              ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.ExportedKeys AS " +
					"  SELECT NULL \"PKTABLE_CAT\",\n" +
					"         \"sUSRFKeyInfo.ref_schema\" \"PKTABLE_SCHEMA\",\n" +
					"         \"sUSRFKeyInfo.ref_table\" \"PKTABLE_NAME\",\n" +
					"         \"sUSRForeignColumns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
					"         NULL \"FKTABLE_CATALOG\",\n" +
					"         \"sUSRFKeyInfo.schema\" \"FKTABLE_SCHEMA\",\n" +
					"         \"sUSRFKeyInfo.table\" \"FKTABLE_NAME\",\n" +
					"         \"sUSRForeignColumns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
					"         \"sUSRForeignColumns.seq_no\" \"KEY_SEQ\",\n" +
					"         I_FRULE_CONVERT(\"sUSRFKeyInfo.update_rule\") \"UPDATE_RULE\",\n" +
					"         I_FRULE_CONVERT(\"sUSRFKeyInfo.delete_rule\") \"DELETE_RULE\",\n" +
					"         \"sUSRFKeyInfo.name\" \"FK_NAME\",\n" +
					"         NULL \"PK_NAME\",\n" +
					"         \"sUSRFKeyInfo.deferred\" \"DEFERRABILITY\"\n" +
					"    FROM SYSTEM.sUSRFKeyInfo, SYSTEM.sUSRForeignColumns\n" +
					"   WHERE sUSRFKeyInfo.id = sUSRForeignColumns.fk_id\n" +
					"     AND \"sUSRFKeyInfo.schema\" IN\n" +
					"              ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.CrossReference AS " +
					"  SELECT NULL \"PKTABLE_CAT\",\n" +
					"         \"sUSRFKeyInfo.ref_schema\" \"PKTABLE_SCHEMA\",\n" +
					"         \"sUSRFKeyInfo.ref_table\" \"PKTABLE_NAME\",\n" +
					"         \"sUSRForeignColumns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
					"         NULL \"FKTABLE_CAT\",\n" +
					"         \"sUSRFKeyInfo.schema\" \"FKTABLE_SCHEMA\",\n" +
					"         \"sUSRFKeyInfo.table\" \"FKTABLE_NAME\",\n" +
					"         \"sUSRForeignColumns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
					"         \"sUSRForeignColumns.seq_no\" \"KEY_SEQ\",\n" +
					"         I_FRULE_CONVERT(\"sUSRFKeyInfo.update_rule\") \"UPDATE_RULE\",\n" +
					"         I_FRULE_CONVERT(\"sUSRFKeyInfo.delete_rule\") \"DELETE_RULE\",\n" +
					"         \"sUSRFKeyInfo.name\" \"FK_NAME\",\n" +
					"         NULL \"PK_NAME\",\n" +
					"         \"sUSRFKeyInfo.deferred\" \"DEFERRABILITY\"\n" +
					"    FROM SYSTEM.sUSRFKeyInfo, SYSTEM.sUSRForeignColumns\n" +
					"   WHERE sUSRFKeyInfo.id = sUSRForeignColumns.fk_id\n" +
					"     AND \"sUSRFKeyInfo.schema\" IN\n" +
					"              ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				// export all the built-in data types...
				stmt.CommandText = 
					"  CREATE VIEW INFORMATION_SCHEMA.DATA_TYPES AS " +
					"  SELECT * FROM SYSTEM.sUSRSQLTypeInfo\n";
				stmt.ExecuteNonQuery();

				//TODO: export the variables too...
			} catch (DataException e) {
				if (e is DbDataException) {
					DbDataException dbDataException = (DbDataException) e;
					Debug.Write(DebugLevel.Error, this, dbDataException.ServerErrorStackTrace);
				}
				Debug.WriteException(DebugLevel.Error, e);
				throw new Exception("SQL Error: " + e.Message);
			}
		}

		/**
		 * Creates all the priv/password system tables.
		 */

		private static void CreateSystemTables(DatabaseConnection connection) {
			// --- The user management tables ---
			DataTableDef sUSRPassword = new DataTableDef();
			sUSRPassword.TableName = SysPassword;
			sUSRPassword.AddColumn(DataTableColumnDef.CreateStringColumn("UserName"));
			sUSRPassword.AddColumn(DataTableColumnDef.CreateStringColumn("Password"));

			DataTableDef sUSRUserPriv = new DataTableDef();
			sUSRUserPriv.TableName = SysUserpriv;
			sUSRUserPriv.AddColumn(DataTableColumnDef.CreateStringColumn("UserName"));
			sUSRUserPriv.AddColumn(
				DataTableColumnDef.CreateStringColumn("PrivGroupName"));

			DataTableDef sUSRUserConnectPriv = new DataTableDef();
			sUSRUserConnectPriv.TableName = SysUserconnect;
			sUSRUserConnectPriv.AddColumn(DataTableColumnDef.CreateStringColumn("UserName"));
			sUSRUserConnectPriv.AddColumn(DataTableColumnDef.CreateStringColumn("Protocol"));
			sUSRUserConnectPriv.AddColumn(DataTableColumnDef.CreateStringColumn("Host"));
			sUSRUserConnectPriv.AddColumn(DataTableColumnDef.CreateStringColumn("Access"));

			DataTableDef sUSRGrant = new DataTableDef();
			sUSRGrant.TableName = SysGrants;
			sUSRGrant.AddColumn(DataTableColumnDef.CreateNumericColumn("priv_bit"));
			sUSRGrant.AddColumn(DataTableColumnDef.CreateNumericColumn("object"));
			sUSRGrant.AddColumn(DataTableColumnDef.CreateStringColumn("param"));
			sUSRGrant.AddColumn(DataTableColumnDef.CreateStringColumn("grantee"));
			sUSRGrant.AddColumn(DataTableColumnDef.CreateStringColumn("grant_option"));
			sUSRGrant.AddColumn(DataTableColumnDef.CreateStringColumn("granter"));

			DataTableDef sUSRService = new DataTableDef();
			sUSRService.TableName = SysService;
			sUSRService.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			sUSRService.AddColumn(DataTableColumnDef.CreateStringColumn("class"));
			sUSRService.AddColumn(DataTableColumnDef.CreateStringColumn("type"));

			DataTableDef sUSRFunctionFactory = new DataTableDef();
			sUSRFunctionFactory.TableName = SysFunctionfactory;
			sUSRFunctionFactory.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			sUSRFunctionFactory.AddColumn(DataTableColumnDef.CreateStringColumn("class"));
			sUSRFunctionFactory.AddColumn(DataTableColumnDef.CreateStringColumn("type"));

			DataTableDef sUSRFunction = new DataTableDef();
			sUSRFunction.TableName = SysFunction;
			sUSRFunction.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			sUSRFunction.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			sUSRFunction.AddColumn(DataTableColumnDef.CreateStringColumn("type"));
			sUSRFunction.AddColumn(DataTableColumnDef.CreateStringColumn("location"));
			sUSRFunction.AddColumn(DataTableColumnDef.CreateStringColumn("return_type"));
			sUSRFunction.AddColumn(DataTableColumnDef.CreateStringColumn("args_type"));
			sUSRFunction.AddColumn(DataTableColumnDef.CreateStringColumn("username"));

			DataTableDef sUSRView = new DataTableDef();
			sUSRView.TableName = SysView;
			sUSRView.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			sUSRView.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			sUSRView.AddColumn(DataTableColumnDef.CreateBinaryColumn("query"));
			sUSRView.AddColumn(DataTableColumnDef.CreateBinaryColumn("data"));
			sUSRView.AddColumn(DataTableColumnDef.CreateStringColumn("username"));

			DataTableDef sUSRLabel = new DataTableDef();
			sUSRLabel.TableName = SysLabel;
			sUSRLabel.AddColumn(DataTableColumnDef.CreateNumericColumn("object_type"));
			sUSRLabel.AddColumn(DataTableColumnDef.CreateStringColumn("object_name"));
			sUSRLabel.AddColumn(DataTableColumnDef.CreateStringColumn("label"));

			DataTableDef sUSRDataTrigger = new DataTableDef();
			sUSRDataTrigger.TableName = SysDataTrigger;
			sUSRDataTrigger.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			sUSRDataTrigger.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			sUSRDataTrigger.AddColumn(DataTableColumnDef.CreateNumericColumn("type"));
			sUSRDataTrigger.AddColumn(DataTableColumnDef.CreateStringColumn("on_object"));
			sUSRDataTrigger.AddColumn(DataTableColumnDef.CreateStringColumn("action"));
			sUSRDataTrigger.AddColumn(DataTableColumnDef.CreateBinaryColumn("misc"));
			sUSRDataTrigger.AddColumn(DataTableColumnDef.CreateStringColumn("username"));

			// Create the tables
			connection.AlterCreateTable(sUSRPassword, 91, 128);
			connection.AlterCreateTable(sUSRUserPriv, 91, 128);
			connection.AlterCreateTable(sUSRUserConnectPriv, 91, 128);
			connection.AlterCreateTable(sUSRGrant, 195, 128);
			connection.AlterCreateTable(sUSRService, 91, 128);
			connection.AlterCreateTable(sUSRFunctionFactory, 91, 128);
			connection.AlterCreateTable(sUSRFunction, 91, 128);
			connection.AlterCreateTable(sUSRView, 91, 128);
			connection.AlterCreateTable(sUSRLabel, 91, 128);
			connection.AlterCreateTable(sUSRDataTrigger, 91, 128);
		}

		///<summary>
		/// Sets all the standard functions and procedures available to engine.
		///</summary>
		///<param name="connection"></param>
		///<param name="admin_user"></param>
		/// <remarks>
		/// This creates an entry in the SysFunction table for all the dynamic
		/// functions and procedures.  This may not include the functions exposed
		/// though the FunctionFactory interface.
		/// </remarks>
		public void SetupSystemFunctions(DatabaseConnection connection, String admin_user) {
			const String GRANTER = InternalSecureUsername;

			// The manager handling the functions.
			ProcedureManager manager = connection.ProcedureManager;

			// Define the SYSTEM_MAKE_BACKUP procedure
			manager.DefineProcedure(
				new ProcedureName(SystemSchema, "SYSTEM_MAKE_BACKUP"),
				"Deveel.Data.Procedure.SystemBackup.Invoke(IProcedureConnection, String)",
				TType.StringType, new TType[] {TType.StringType},
				admin_user);

			// -----

			// Set the grants for the procedures.
			GrantManager grants = connection.GrantManager;

			// Revoke all existing grants on the internal stored procedures.
			grants.RevokeAllGrantsOnObject(GrantObject.Table,
			                               "SYSTEM.SYSTEM_MAKE_BACKUP");

			// Grant execute priv with grant option to administrator
			grants.Grant(Privileges.ProcedureExecute,
			                GrantObject.Table,
			                "SYSTEM.SYSTEM_MAKE_BACKUP",
			                admin_user, true, GRANTER);
		}

		/**
		 * Clears all the grant information in the sUSRGrant table.  This should only
		 * be used if we need to refresh the grant information for whatever reason
		 * (such as when converting between different versions).
		 */

		private static void ClearAllGrants(DatabaseConnection connection) {
			DataTable grant_table = connection.GetTable(SysGrants);
			grant_table.Delete(grant_table);
		}

		/// <summary>
		/// Set up the system table grants.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="grantee"></param>
		/// <remarks>
		/// This gives the grantee user full access to <i>passwords</i>,
		/// <i>user_privs</i>, <i>user_connect_privs</i>, <i>services</i>, 
		/// <i>function_factories</i>, and functions. All other system 
		/// tables are granted <i>SELECT</i> only.
		/// </remarks>
		private static void SetSystemGrants(DatabaseConnection connection, String grantee) {
			const string GRANTER = InternalSecureUsername;

			// Add all priv grants to those that the system user is allowed to change
			GrantManager manager = connection.GrantManager;

			// Add schema grant for APP
			manager.Grant(Privileges.SchemaAll, GrantObject.Schema, "APP", grantee, true, GRANTER);
			// Add public grant for SYSTEM
			manager.Grant(Privileges.SchemaRead, GrantObject.Schema, "SYSTEM", GrantManager.PublicUsernameStr, false, GRANTER);
			// Add public grant for INFORMATION_SCHEMA
			manager.Grant(Privileges.SchemaRead, GrantObject.Schema, "INFORMATION_SCHEMA", GrantManager.PublicUsernameStr, false,
			              GRANTER);

			// For all tables in the SYSTEM schema, grant all privileges to the
			// system user.
			manager.GrantToAllTablesInSchema("SYSTEM", Privileges.TableAll, grantee, false, GRANTER);

			// Set the public grants for the system tables,
			manager.Grant(Privileges.TableRead, GrantObject.Table, "SYSTEM.sUSRConnectionInfo", GrantManager.PublicUsernameStr,
			              false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "SYSTEM.sUSRCurrentConnections",
			              GrantManager.PublicUsernameStr, false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "SYSTEM.sUSRVariables", GrantManager.PublicUsernameStr, false,
			              GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "SYSTEM.sUSRDatabaseStatistics",
			              GrantManager.PublicUsernameStr, false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "SYSTEM.sUSRDatabaseVars", GrantManager.PublicUsernameStr,
			              false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "SYSTEM.sUSRProductInfo", GrantManager.PublicUsernameStr,
			              false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "SYSTEM.sUSRSQLTypeInfo", GrantManager.PublicUsernameStr,
			              false, GRANTER);

			// Set public grants for the system views.
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserGrant",
			              GrantManager.PublicUsernameStr, false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserSimpleGrant",
			              GrantManager.PublicUsernameStr, false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserSchemaInfo",
			              GrantManager.PublicUsernameStr, false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserTableColumns",
			              GrantManager.PublicUsernameStr, false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ThisUserTableInfo",
			              GrantManager.PublicUsernameStr, false, GRANTER);

			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.TABLES", GrantManager.PublicUsernameStr,
			              false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.SCHEMATA", GrantManager.PublicUsernameStr,
			              false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.CATALOGS", GrantManager.PublicUsernameStr,
			              false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.COLUMNS", GrantManager.PublicUsernameStr,
			              false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.COLUMN_PRIVILEGES",
			              GrantManager.PublicUsernameStr, false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.TABLE_PRIVILEGES",
			              GrantManager.PublicUsernameStr, false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.PrimaryKeys",
			              GrantManager.PublicUsernameStr, false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ImportedKeys",
			              GrantManager.PublicUsernameStr, false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.ExportedKeys",
			              GrantManager.PublicUsernameStr, false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "INFORMATION_SCHEMA.CrossReference",
			              GrantManager.PublicUsernameStr, false, GRANTER);
		}

		/// <summary>
		/// Creates and sets up a new database to an initial empty state. 
		/// </summary>
		/// <param name="username">The username for the database administrator.</param>
		/// <param name="password">The database administrator password.</param>
		/// <remarks>
		/// The creation process involves creating all the system tables and 
		/// views, adding an administrator user account, creating schema, and 
		/// setting up the initial grant information for the administrator user.
		/// </remarks>
		public void Create(String username, String password) {
			if (IsReadOnly) {
				throw new Exception("Can not create database in Read only mode.");
			}

			if (username == null || username.Length == 0 ||
			    password == null || password.Length == 0) {
				throw new Exception("Must have valid username and password String");
			}

			try {
				// Create the conglomerate
				conglomerate.Create(Name);

				DatabaseConnection connection = CreateNewConnection(null, null);
				DatabaseQueryContext context = new DatabaseQueryContext(connection);
				connection.LockingMechanism.SetMode(LockingMode.Exclusive);
				connection.CurrentSchema = SystemSchema;

				// Create the schema information tables introduced in version 0.90
				// and 0.94
				CreateSchemaInfoTables(connection);

				// The system tables that are present in every conglomerate.
				CreateSystemTables(connection);
				// Create the system views
				CreateSystemViews(connection);

				// Creates the administrator user.
				CreateUser(context, username, password);
				// This is the admin user so add to the 'secure access' table.
				AddUserToGroup(context, username, SecureGroup);
				// Allow all localhost TCP connections.
				// NOTE: Permissive initial security!
				GrantHostAccessToUser(context, username, "TCP", "%");
				// Allow all Local connections (from within JVM).
				GrantHostAccessToUser(context, username, "Local", "%");

				// Sets the system grants for the administrator
				SetSystemGrants(connection, username);

				// Set all default system procedures.
				SetupSystemFunctions(connection, username);

				try {
					// Close and commit this transaction.
					connection.Commit();
				} catch (TransactionException e) {
					Debug.WriteException(e);
					throw new ApplicationException("Transaction Error: " + e.Message);
				}

				connection.LockingMechanism.FinishMode(
					LockingMode.Exclusive);
				connection.Close();

				// Close the conglomerate.
				conglomerate.Close();
			} catch (DatabaseException e) {
				Debug.WriteException(e);
				throw new ApplicationException("Database Exception: " + e.Message);
			} catch (IOException e) {
				Debug.WriteException(e);
				throw new ApplicationException("IO Error: " + e.Message);
			}
		}

		/// <summary>
		/// Opens and initializes the database.
		/// </summary>
		/// <remarks>
		/// This opens all the files that are required for the operation 
		/// of the database.
		/// </remarks>
		/// <exception cref="DatabaseException">
		/// If it finds the version of the data files are not a compatible 
		/// version or if the database is already opened.
		/// </exception>
		/// <exception cref="IOException">
		/// If any IO error occurred during the opening process.
		/// </exception>
		public void Init() {
			if (initialised) {
				throw new Exception("Init() method can only be called once.");
			}

			// Reset all session statistics.
			Stats.ResetSession();

			try {
				string log_path = system.LogDirectory;
				if (log_path != null && system.LogQueries) {
					commands_log = new Log(Path.Combine(log_path, "commands.log"), 256*1024, 5);
				} else {
					commands_log = Log.Null;
				}

				// Check if the state file exists.  If it doesn't, we need to report
				// incorrect version.
				if (!StoreSystem.StoreExists(Name + "_sf")) {
					// If state store doesn't exist but the legacy style '.sf' state file
					// exists,
					if (system.DatabasePath != null &&
					    File.Exists(Path.Combine(system.DatabasePath, Name + ".sf"))) {
						throw new DatabaseException(
							"The state store for this database doesn't exist.  This means " +
							"the database version is pre version 1.0.  Please see the " +
							"README for the details for converting this database.");
					} else {
						// If neither store or state file exist, assume database doesn't
						// exist.
						throw new DatabaseException("The database does not exist.");
					}
				}

				// Open the conglomerate
				conglomerate.Open(Name);

				// Check the state of the conglomerate,
				DatabaseConnection connection = CreateNewConnection(null, null);
				DatabaseQueryContext context = new DatabaseQueryContext(connection);
				connection.LockingMechanism.SetMode(LockingMode.Exclusive);
				if (!connection.TableExists(TableDataConglomerate.PERSISTENT_VAR_TABLE)) {
					throw new DatabaseException(
						"The sUSRDatabaseVars table doesn't exist.  This means the " +
						"database is pre-schema version 1 or the table has been deleted." +
						"If you are converting an old version of the database, please " +
						"convert the database using an older release.");
				}

				// What version is the data?
				DataTable database_vars =
					connection.GetTable(TableDataConglomerate.PERSISTENT_VAR_TABLE);
				IDictionary vars = database_vars.ToDictionary();
				String db_version = vars["database.version"].ToString();
				// If the version doesn't equal the current version, throw an error.
				if (!db_version.Equals("1.4")) {
					throw new DatabaseException(
						"Incorrect data file version '" + db_version + "'.  Please see " +
						"the README on how to convert the data files to the current " +
						"version.");
				}

				// Commit and close the connection.
				connection.Commit();
				connection.LockingMechanism.FinishMode(LockingMode.Exclusive);
				connection.Close();
			} catch (TransactionException e) {
				// This would be very strange error to receive for in initializing
				// database...
				throw new ApplicationException("Transaction Error: " + e.Message);
			} catch (IOException e) {
				Console.Error.WriteLine(e.Message);
				Console.Error.WriteLine(e.StackTrace);
				throw new ApplicationException("IO Error: " + e.Message);
			}

			initialised = true;
		}

		/// <summary>
		/// Cleanly shuts down the database.
		/// </summary>
		/// <remarks>
		/// It is important that this method is called just before the system 
		/// closes down.
		/// <para>
		/// The main purpose of this method is to ensure any tables that are 
		/// backed by files and in a <i>safe</i> state and cleanly flushed to 
		/// the file system.
		/// </para>
		/// <para>
		/// If <see cref="delete_on_shutdown"/> is true, the database will delete itself from the file 
		/// system when it shuts down.
		/// </para>
		/// </remarks>
		public void Shutdown() {
			if (initialised == false) {
				throw new ApplicationException("The database is not initialized.");
			}

			try {
				if (delete_on_shutdown == true) {
					// Delete the conglomerate if the database is set to delete on
					// shutdown.
					conglomerate.Delete();
				} else {
					// Otherwise close the conglomerate.
					conglomerate.Close();
				}
			} catch (IOException e) {
				Debug.WriteException(e);
				throw new ApplicationException("IO Error: " + e.Message);
			}

			// Shut down the logs...
			if (commands_log != null) {
				commands_log.Close();
			}

			initialised = false;
		}

		///<summary>
		/// If the 'delete_on_shutdown' flag is set, the database will delete 
		/// the database from the file system when it is shutdown.
		///</summary>
		///<param name="status"></param>
		/// <remarks>
		/// <b>Note</b>: Use with care - if this is set to true and the database is 
		/// shutdown it will result in total loss of data.
		/// </remarks>
		public void SetDeleteOnShutdown(bool status) {
			delete_on_shutdown = status;
		}

		/// <summary>
		/// Copies all the persistent data in this database (the conglomerate) to 
		/// the given destination path.
		/// </summary>
		/// <param name="path">The destination path of the copy.</param>
		/// <remarks>
		///  This method can copy information while the database is <i>live</i>.
		/// </remarks>
		public void LiveCopyTo(string path) {
			if (initialised == false) {
				throw new ApplicationException("The database is not initialized.");
			}

			// Set up the destination conglomerate to copy all the data to,
			// Note that this sets up a typical destination conglomerate and changes
			// the cache size and disables the debug log.
			TransactionSystem copy_system = new TransactionSystem();
			DefaultDbConfig config = new DefaultDbConfig();
			config.DatabasePath = Path.GetFullPath(path);
			config.LogPath = "";
			config.MinimumDebugLevel = 50000;
			// Set data cache to 1MB
			config.SetValue("data_cache_size", "1048576");
			// Set io_safety_level to 1 for destination database
			// ISSUE: Is this a good assumption to make - 
			//     we don't care if changes are lost by a power failure when we are
			//     backing up the database.  Even if journalling is enabled, a power
			//     failure will lose changes in the backup copy anyway.
			config.SetValue("io_safety_level", "1");
			config.SetValue("debug_logs", "disabled");
			copy_system.Init(config);
			TableDataConglomerate dest_conglomerate = new TableDataConglomerate(copy_system, copy_system.StoreSystem);

			// Open the congloemrate
			dest_conglomerate.MinimalCreate(Name);

			try {
				// Make a copy of this conglomerate into the destination conglomerate,
				conglomerate.LiveCopyTo(dest_conglomerate);
			} finally {
				// Close the congloemrate when finished.
				dest_conglomerate.Close();
				// Dispose the TransactionSystem
				copy_system.Dispose();
			}
		}

		// ---------- Server side procedures ----------

		///<summary>
		/// Resolves a procedure name into a <see cref="IDatabaseProcedure"/> object.
		///</summary>
		///<param name="procedure_name"></param>
		///<param name="connection"></param>
		/// <remarks>
		/// This is used for finding a server side script.
		/// </remarks>
		///<returns></returns>
		///<exception cref="DatabaseException">
		/// If the procedure could not be resolved or there was an error retrieving it.
		/// </exception>
		public IDatabaseProcedure GetDbProcedure(String procedure_name, DatabaseConnection connection) {
			// The procedure we are getting.
			IDatabaseProcedure procedure_instance;

			try {
				// Find the procedure
				Type proc = Type.GetType("Deveel.Data.Procedure." + procedure_name);
				// Instantiate a new instance of the procedure
				procedure_instance = (IDatabaseProcedure) Activator.CreateInstance(proc);

				Debug.Write(DebugLevel.Information, this, "Getting raw class file: " + procedure_name);
			} catch (AccessViolationException e) {
				Debug.WriteException(e);
				throw new DatabaseException("Illegal Access: " + e.Message);
			} catch (TypeInitializationException e) {
				Debug.WriteException(e);
				throw new DatabaseException("Instantiation Error: " + e.Message);
			} catch (TypeLoadException e) {
				Debug.WriteException(e);
				throw new DatabaseException("Class Not Found: " + e.Message);
			}

			// Return the procedure.
			return procedure_instance;
		}

		// ---------- System access ----------

		/// <summary>
		/// Creates an event for the database dispatcher.
		/// </summary>
		/// <param name="runner"></param>
		/// <returns></returns>
		public Object CreateEvent(IDatabaseEvent runner) {
			return System.CreateEvent(runner);
		}

		/// <summary>
		/// Creates an event for the database dispatcher.
		/// </summary>
		/// <param name="runner"></param>
		/// <returns></returns>
		public Object CreateEvent(EventHandler runner) {
			return CreateEvent(new DatabaseEventHandler(runner));
		}

		/// <summary>
		/// Posts an event on the database dispatcher.
		/// </summary>
		/// <param name="time"></param>
		/// <param name="e"></param>
		public void PostEvent(int time, Object e) {
			System.PostEvent(time, e);
		}


		/// <summary>
		/// Starts the shutdown thread which should contain delegates that shut the
		/// database and all its resources down.
		/// </summary>
		/// <remarks>
		/// This method returns immediately.
		/// </remarks>
		public void StartShutDownThread() {
			System.StartShutDownThread();
		}

		/// <summary>
		/// Blocks until the database has shut down.
		/// </summary>
		public void WaitUntilShutdown() {
			System.WaitUntilShutdown();
		}

		/// <summary>
		/// Executes database functions from the given 
		/// delegate on the first available worker thread.
		/// </summary>
		/// <param name="user"></param>
		/// <param name="database"></param>
		/// <param name="runner"></param>
		/// <remarks>
		/// All database functions must go through a worker thread.  If we 
		/// ensure this, we can easily stop all database functions from executing 
		/// if need be.  Also, we only need to have a certain number of threads 
		/// active at any one time rather than a unique thread for each connection.
		/// </remarks>
		public void Execute(User user, DatabaseConnection database, IDatabaseEvent runner) {
			System.Execute(user, database, runner);
		}

		/// <summary>
		/// Executes database functions from the given 
		/// delegate on the first available worker thread.
		/// </summary>
		/// <param name="user"></param>
		/// <param name="database"></param>
		/// <param name="runner"></param>
		/// <seealso cref="Execute(Deveel.Data.User,Deveel.Data.DatabaseConnection,Deveel.Data.IDatabaseEvent)"/>
		public void Execute(User user, DatabaseConnection database, EventHandler runner) {
			Execute(user, database, new DatabaseEventHandler(runner));
		}

		/// <summary>
		/// Registers the delegate that is executed when the shutdown 
		/// thread is activated.
		/// </summary>
		/// <param name="d"></param>
		public void RegisterShutDownDelegate(EventHandler d) {
			RegisterShutDownDelegate(new DatabaseEventHandler(d));
		}

		/// <summary>
		/// Registers the delegate that is executed when the shutdown 
		/// thread is activated.
		/// </summary>
		/// <param name="d"></param>
		public void RegisterShutDownDelegate(IDatabaseEvent e) {
			System.RegisterShutDownDelegate(e);
		}

		/// <summary>
		/// Controls whether the database is allowed to execute commands or not.
		/// </summary>
		/// <remarks>
		/// If this is set to true, then calls to 'execute' will be executed
		/// as soon as there is a free worker thread available.  Otherwise no
		/// commands are executed until this is enabled.
		/// </remarks>
		public void SetIsExecutingCommands(bool status) {
			System.SetIsExecutingCommands(status);
		}


		// ---------- Static methods ----------

		/// <summary>
		/// Given the sUSRDatabaseVars table, this will update the given key with
		/// the given value in the table in the current transaction.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="database_vars"></param>
		/// <param name="key"></param>
		/// <param name="value"></param>
		private static void UpdateDatabaseVars(IQueryContext context,
		                                       DataTable database_vars, String key, String value) {
			// The references to the first and second column (key/value)
			VariableName c1 = database_vars.GetResolvedVariable(0); // First column
			VariableName c2 = database_vars.GetResolvedVariable(1); // Second column

			// Assignment: second column = value
			Assignment assignment = new Assignment(c2, new Expression(TObject.GetString(value)));
			// All rows from database_vars where first column = the key
			Table t1 = database_vars.SimpleSelect(context, c1, Operator.Get("="), new Expression(TObject.GetString(key)));

			// Update the variable
			database_vars.Update(context, t1, new Assignment[] {assignment}, -1);
		}

		private void Dispose() {
			if (IsInitialized) {
				Console.Error.WriteLine("Database object was finalized and is initialized!");
			}
		}

		#region Implementation of IDisposable

		void IDisposable.Dispose() {
			GC.SuppressFinalize(this);
			Dispose();
		}

		#endregion
	}
}