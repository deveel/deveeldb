// 
//  Copyright 2010  Deveel
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
	public sealed partial class Database : IDisposable {
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
		public static readonly TableName SysDataTrigger = new TableName(SystemSchema, "data_trigger");

		/// <summary>
		/// The system internally generated 'database_stats' table.
		/// </summary>
		public static readonly TableName SysDbStatistics = new TableName(SystemSchema, "database_stats");

		/// <summary>
		/// The function table.
		/// </summary>
		public static readonly TableName SysFunction = new TableName(SystemSchema, "function");

		/// <summary>
		/// The function factory table.
		/// </summary>
		public static readonly TableName SysFunctionfactory = new TableName(SystemSchema, "function_factory");

		///<summary>
		///</summary>
		public static readonly TableName SysGrants = new TableName(SystemSchema, "grant");

		/// <summary>
		/// The label table.
		/// </summary>
		public static readonly TableName SysLabel = new TableName(SystemSchema, "label");

		/// <summary>
		/// The password privs and grants table.
		/// </summary>
		public static readonly TableName SysPassword = new TableName(SystemSchema, "password");

		/// <summary>
		/// The services table.
		/// </summary>
		public static readonly TableName SysService = new TableName(SystemSchema, "service");

		/// <summary>
		/// The system internally generated 'table_columns' table.
		/// </summary>
		public static readonly TableName SysTableColumns = new TableName(SystemSchema, "table_columns");

		/// <summary>
		/// The system internally generated 'table_info' table.
		/// </summary>
		public static readonly TableName SysTableInfo = new TableName(SystemSchema, "table_info");

		///<summary>
		///</summary>
		public static readonly TableName SysUserConnect = new TableName(SystemSchema, "user_connect_priv");

		///<summary>
		///</summary>
		public static readonly TableName SysUserPriv = new TableName(SystemSchema, "user_priv");

		/// <summary>
		/// The view table.
		/// </summary>
		public static readonly TableName SysView = new TableName(SystemSchema, "view");

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
		private readonly User internalSystemUser;

		/// <summary>
		/// The name of this database.
		/// </summary>
		private readonly String name;

		/// <summary>
		/// A table that has a single row but no columns.
		/// </summary>
		private readonly Table singleRowTable;

		/// <summary>
		/// The DatabaseSystem that this database is part of.
		/// </summary>
		private readonly DatabaseSystem system;

		/// <summary>
		/// The database wide TriggerManager object that dispatches trigger events
		/// to the DatabaseConnection objects that are listening for the events.
		/// </summary>
		private readonly TriggerManager triggerManager;

		/// <summary>
		/// This log file records the SQL commands executed on the server.
		/// </summary>
		private Log commandsLog;

		/// <summary>
		/// A flag which, when set to true, will cause the engine to delete the
		/// database from the file system when it is shut down.
		/// </summary>
		private bool deleteOnShutdown;

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
			deleteOnShutdown = false;
			this.name = name;
			system.RegisterDatabase(this);
			conglomerate = new TableDataConglomerate(system, system.StoreSystem);
			internalSystemUser = new User(InternalSecureUsername, this, "", DateTime.Now);

			// Create the single row table
			TemporaryTable t = new TemporaryTable(this,"SINGLE_ROW_TABLE", new DataTableColumnDef[0]);
			t.NewRow();
			singleRowTable = t;

			triggerManager = new TriggerManager(system);
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
			get { return internalSystemUser; }
		}

		// ---------- Log accesses ----------

		/// <summary>
		/// Returns the log file where commands are recorded.
		/// </summary>
		public Log CommandsLog {
			get { return commandsLog; }
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
			get { return triggerManager; }
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
			get { return singleRowTable; }
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
					"    FROM SYSTEM.grant " +
					"   WHERE ( grantee = user() OR grantee = '@PUBLIC' )";
				stmt.ExecuteNonQuery();

				// This view shows the grants that the user is allowed to see
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserGrant AS " +
					"  SELECT \"description\", \"object\", \"param\", \"grantee\", " +
					"         \"grant_option\", \"granter\" " +
					"    FROM SYSTEM.grant, SYSTEM.sUSRPrivMap " +
					"   WHERE ( grantee = user() OR grantee = '@PUBLIC' )" +
					"     AND grant.priv_bit = sUSRPrivMap.priv_bit";
				stmt.ExecuteNonQuery();

				// A view that represents the list of schema this user is allowed to view
				// the contents of.
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserSchemaInfo AS " +
					"  SELECT * FROM SYSTEM.schema_info " +
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
					"  SELECT * FROM SYSTEM.table_columns " +
					"   WHERE \"schema\" IN ( " +
					"     SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )";
				stmt.ExecuteNonQuery();

				// A view that exposes the 'table_info' table but only for the tables
				// this user has Read access to.
				stmt.CommandText =
					"CREATE VIEW INFORMATION_SCHEMA.ThisUserTableInfo AS " +
					"  SELECT * FROM SYSTEM.table_info " +
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
					"    FROM SYSTEM.schema_info\n" + // Hacky, this will generate a 0 row
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
					"         \"SYSTEM.primary_columns.seq_no\" \"KEY_SEQ\",\n" +
					"         \"name\" \"PK_NAME\"\n" +
					"    FROM SYSTEM.pkey_info, SYSTEM.primary_columns\n" +
					"   WHERE pkey_info.id = primary_columns.pk_id\n" +
					"     AND \"schema\" IN\n" +
					"            ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.ImportedKeys AS " +
					"  SELECT NULL \"PKTABLE_CATALOG\",\n" +
					"         \"fkey_info.ref_schema\" \"PKTABLE_SCHEMA\",\n" +
					"         \"fkey_info.ref_table\" \"PKTABLE_NAME\",\n" +
					"         \"foreign_columns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
					"         NULL \"FKTABLE_CATALOG\",\n" +
					"         \"fkey_info.schema\" \"FKTABLE_SCHEMA\",\n" +
					"         \"fkey_info.table\" \"FKTABLE_NAME\",\n" +
					"         \"foreign_columns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
					"         \"foreign_columns.seq_no\" \"KEY_SEQ\",\n" +
					"         I_FRULE_CONVERT(\"fkey_info.update_rule\") \"UPDATE_RULE\",\n" +
					"         I_FRULE_CONVERT(\"fkey_info.delete_rule\") \"DELETE_RULE\",\n" +
					"         \"fkey_info.name\" \"FK_NAME\",\n" +
					"         NULL \"PK_NAME\",\n" +
					"         \"fkey_info.deferred\" \"DEFERRABILITY\"\n" +
					"    FROM SYSTEM.fkey_info, SYSTEM.foreign_columns\n" +
					"   WHERE fkey_info.id = foreign_columns.fk_id\n" +
					"     AND \"fkey_info.schema\" IN\n" +
					"              ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.ExportedKeys AS " +
					"  SELECT NULL \"PKTABLE_CAT\",\n" +
					"         \"fkey_info.ref_schema\" \"PKTABLE_SCHEMA\",\n" +
					"         \"fkey_info.ref_table\" \"PKTABLE_NAME\",\n" +
					"         \"foreign_columns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
					"         NULL \"FKTABLE_CATALOG\",\n" +
					"         \"fkey_info.schema\" \"FKTABLE_SCHEMA\",\n" +
					"         \"fkey_info.table\" \"FKTABLE_NAME\",\n" +
					"         \"foreign_columns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
					"         \"foreign_columns.seq_no\" \"KEY_SEQ\",\n" +
					"         I_FRULE_CONVERT(\"fkey_info.update_rule\") \"UPDATE_RULE\",\n" +
					"         I_FRULE_CONVERT(\"fkey_info.delete_rule\") \"DELETE_RULE\",\n" +
					"         \"fkey_info.name\" \"FK_NAME\",\n" +
					"         NULL \"PK_NAME\",\n" +
					"         \"fkey_info.deferred\" \"DEFERRABILITY\"\n" +
					"    FROM SYSTEM.fkey_info, SYSTEM.foreign_columns\n" +
					"   WHERE fkey_info.id = foreign_columns.fk_id\n" +
					"     AND \"fkey_info.schema\" IN\n" +
					"              ( SELECT \"name\" FROM INFORMATION_SCHEMA.ThisUserSchemaInfo )\n";
				stmt.ExecuteNonQuery();

				stmt.CommandText =
					"  CREATE VIEW INFORMATION_SCHEMA.CrossReference AS " +
					"  SELECT NULL \"PKTABLE_CAT\",\n" +
					"         \"fkey_info.ref_schema\" \"PKTABLE_SCHEMA\",\n" +
					"         \"fkey_info.ref_table\" \"PKTABLE_NAME\",\n" +
					"         \"foreign_columns.pcolumn\" \"PKCOLUMN_NAME\",\n" +
					"         NULL \"FKTABLE_CAT\",\n" +
					"         \"fkey_info.schema\" \"FKTABLE_SCHEMA\",\n" +
					"         \"fkey_info.table\" \"FKTABLE_NAME\",\n" +
					"         \"foreign_columns.fcolumn\" \"FKCOLUMN_NAME\",\n" +
					"         \"foreign_columns.seq_no\" \"KEY_SEQ\",\n" +
					"         I_FRULE_CONVERT(\"fkey_info.update_rule\") \"UPDATE_RULE\",\n" +
					"         I_FRULE_CONVERT(\"fkey_info.delete_rule\") \"DELETE_RULE\",\n" +
					"         \"fkey_info.name\" \"FK_NAME\",\n" +
					"         NULL \"PK_NAME\",\n" +
					"         \"fkey_info.deferred\" \"DEFERRABILITY\"\n" +
					"    FROM SYSTEM.fkey_info, SYSTEM.foreign_columns\n" +
					"   WHERE fkey_info.id = foreign_columns.fk_id\n" +
					"     AND \"fkey_info.schema\" IN\n" +
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
			DataTableDef password = new DataTableDef();
			password.TableName = SysPassword;
			password.AddColumn(DataTableColumnDef.CreateStringColumn("UserName"));
			password.AddColumn(DataTableColumnDef.CreateStringColumn("Password"));

			DataTableDef userPriv = new DataTableDef();
			userPriv.TableName = SysUserPriv;
			userPriv.AddColumn(DataTableColumnDef.CreateStringColumn("UserName"));
			userPriv.AddColumn(DataTableColumnDef.CreateStringColumn("PrivGroupName"));

			DataTableDef userConnectPriv = new DataTableDef();
			userConnectPriv.TableName = SysUserConnect;
			userConnectPriv.AddColumn(DataTableColumnDef.CreateStringColumn("UserName"));
			userConnectPriv.AddColumn(DataTableColumnDef.CreateStringColumn("Protocol"));
			userConnectPriv.AddColumn(DataTableColumnDef.CreateStringColumn("Host"));
			userConnectPriv.AddColumn(DataTableColumnDef.CreateStringColumn("Access"));

			DataTableDef grant = new DataTableDef();
			grant.TableName = SysGrants;
			grant.AddColumn(DataTableColumnDef.CreateNumericColumn("priv_bit"));
			grant.AddColumn(DataTableColumnDef.CreateNumericColumn("object"));
			grant.AddColumn(DataTableColumnDef.CreateStringColumn("param"));
			grant.AddColumn(DataTableColumnDef.CreateStringColumn("grantee"));
			grant.AddColumn(DataTableColumnDef.CreateStringColumn("grant_option"));
			grant.AddColumn(DataTableColumnDef.CreateStringColumn("granter"));

			DataTableDef service = new DataTableDef();
			service.TableName = SysService;
			service.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			service.AddColumn(DataTableColumnDef.CreateStringColumn("class"));
			service.AddColumn(DataTableColumnDef.CreateStringColumn("type"));

			DataTableDef functionFactory = new DataTableDef();
			functionFactory.TableName = SysFunctionfactory;
			functionFactory.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			functionFactory.AddColumn(DataTableColumnDef.CreateStringColumn("class"));
			functionFactory.AddColumn(DataTableColumnDef.CreateStringColumn("type"));

			DataTableDef function = new DataTableDef();
			function.TableName = SysFunction;
			function.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			function.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			function.AddColumn(DataTableColumnDef.CreateStringColumn("type"));
			function.AddColumn(DataTableColumnDef.CreateStringColumn("location"));
			function.AddColumn(DataTableColumnDef.CreateStringColumn("return_type"));
			function.AddColumn(DataTableColumnDef.CreateStringColumn("args_type"));
			function.AddColumn(DataTableColumnDef.CreateStringColumn("username"));

			DataTableDef view = new DataTableDef();
			view.TableName = SysView;
			view.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			view.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			view.AddColumn(DataTableColumnDef.CreateBinaryColumn("query"));
			view.AddColumn(DataTableColumnDef.CreateBinaryColumn("data"));
			view.AddColumn(DataTableColumnDef.CreateStringColumn("username"));

			DataTableDef label = new DataTableDef();
			label.TableName = SysLabel;
			label.AddColumn(DataTableColumnDef.CreateNumericColumn("object_type"));
			label.AddColumn(DataTableColumnDef.CreateStringColumn("object_name"));
			label.AddColumn(DataTableColumnDef.CreateStringColumn("label"));

			DataTableDef dataTrigger = new DataTableDef();
			dataTrigger.TableName = SysDataTrigger;
			dataTrigger.AddColumn(DataTableColumnDef.CreateStringColumn("schema"));
			dataTrigger.AddColumn(DataTableColumnDef.CreateStringColumn("name"));
			dataTrigger.AddColumn(DataTableColumnDef.CreateNumericColumn("type"));
			dataTrigger.AddColumn(DataTableColumnDef.CreateStringColumn("on_object"));
			dataTrigger.AddColumn(DataTableColumnDef.CreateStringColumn("action"));
			dataTrigger.AddColumn(DataTableColumnDef.CreateBinaryColumn("misc"));
			dataTrigger.AddColumn(DataTableColumnDef.CreateStringColumn("username"));

			// Create the tables
			connection.AlterCreateTable(password, 91, 128);
			connection.AlterCreateTable(userPriv, 91, 128);
			connection.AlterCreateTable(userConnectPriv, 91, 128);
			connection.AlterCreateTable(grant, 195, 128);
			connection.AlterCreateTable(service, 91, 128);
			connection.AlterCreateTable(functionFactory, 91, 128);
			connection.AlterCreateTable(function, 91, 128);
			connection.AlterCreateTable(view, 91, 128);
			connection.AlterCreateTable(label, 91, 128);
			connection.AlterCreateTable(dataTrigger, 91, 128);
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
			manager.Grant(Privileges.TableRead, GrantObject.Table, "SYSTEM.database_stats",
			              GrantManager.PublicUsernameStr, false, GRANTER);
			manager.Grant(Privileges.TableRead, GrantObject.Table, "SYSTEM.database_vars", GrantManager.PublicUsernameStr,
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

			if (String.IsNullOrEmpty(username) ||
			    String.IsNullOrEmpty(password))
				throw new Exception("Must have valid username and password String");

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

				connection.LockingMechanism.FinishMode(LockingMode.Exclusive);
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
			if (initialised)
				throw new Exception("Init() method can only be called once.");

			// Reset all session statistics.
			Stats.ResetSession();

			try {
				string logPath = system.LogDirectory;
				if (logPath != null && system.LogQueries) {
					commandsLog = new Log(Path.Combine(logPath, "commands.log"), 256*1024, 5);
				} else {
					commandsLog = Log.Null;
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
				if (!connection.TableExists(TableDataConglomerate.PersistentVarTable)) {
					throw new DatabaseException(
						"The database_vars table doesn't exist.  This means the " +
						"database is pre-schema version 1 or the table has been deleted." +
						"If you are converting an old version of the database, please " +
						"convert the database using an older release.");
				}

				// What version is the data?
				DataTable database_vars =
					connection.GetTable(TableDataConglomerate.PersistentVarTable);
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
		/// If <see cref="deleteOnShutdown"/> is true, the database will delete itself from the file 
		/// system when it shuts down.
		/// </para>
		/// </remarks>
		public void Shutdown() {
			if (initialised == false) {
				throw new ApplicationException("The database is not initialized.");
			}

			try {
				if (deleteOnShutdown == true) {
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
			if (commandsLog != null) {
				commandsLog.Close();
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
			deleteOnShutdown = status;
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
			DbConfig config = DbConfig.Default;
			config.DatabasePath = Path.GetFullPath(path);
			config.LogPath = "";
			config.DebugLevel = 50000;
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
		public object CreateEvent(EventHandler runner) {
			return System.CreateEvent(runner);
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
		public void Execute(User user, DatabaseConnection database, EventHandler runner) {
			System.Execute(user, database, runner);
		}

		/// <summary>
		/// Registers the delegate that is executed when the shutdown 
		/// thread is activated.
		/// </summary>
		/// <param name="e"></param>
		public void RegisterShutDownDelegate(EventHandler e) {
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
		/// Given the database_vars table, this will update the given key with
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


		#region Implementation of IDisposable

		public void Dispose() {
			if (IsInitialized) {
				Console.Error.WriteLine("Database object was finalized and is initialized!");
			}

			GC.SuppressFinalize(this);
		}

		#endregion
	}
}