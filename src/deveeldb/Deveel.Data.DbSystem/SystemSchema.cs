using System;
using System.Collections.Generic;
using System.IO;

using Deveel.Data.Procedures;
using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Transactions;
using Deveel.Data.Types;
using Deveel.Data.Util;

namespace Deveel.Data.DbSystem {
	public static class SystemSchema {
		/// <summary>
		/// The name of the system schema that contains tables refering to 
		/// system information.
		/// </summary>
		public const string Name = "SYSTEM";

		/// <summary>
		/// The system internally generated 'data_trigger' table.
		/// </summary>
		public static readonly TableName DataTrigger = new TableName(Name, "data_trigger");

		/// <summary>
		/// The system internally generated 'database_stats' table.
		/// </summary>
		public static readonly TableName DatabaseStatistics = new TableName(Name, "database_stats");

		/// <summary>
		/// The function table.
		/// </summary>
		public static readonly TableName Function = new TableName(Name, "function");

		/// <summary>
		/// The function factory table.
		/// </summary>
		public static readonly TableName Functionfactory = new TableName(Name, "function_factory");

		///<summary>
		///</summary>
		public static readonly TableName Grant = new TableName(Name, "grant");

		/// <summary>
		/// The label table.
		/// </summary>
		public static readonly TableName Label = new TableName(Name, "label");

		/// <summary>
		/// The password privs and grants table.
		/// </summary>
		public static readonly TableName Password = new TableName(Name, "password");

		/// <summary>
		/// The services table.
		/// </summary>
		public static readonly TableName Service = new TableName(Name, "service");

		/// <summary>
		/// The system internally generated 'table_columns' table.
		/// </summary>
		public static readonly TableName TableColumns = new TableName(Name, "table_columns");

		/// <summary>
		/// The system internally generated 'table_info' table.
		/// </summary>
		public static readonly TableName TableInfo = new TableName(Name, "table_info");

		///<summary>
		///</summary>
		public static readonly TableName UserConnectPrivileges = new TableName(Name, "user_connect_priv");

		///<summary>
		///</summary>
		public static readonly TableName UserPrivileges = new TableName(Name, "user_priv");

		/// <summary>
		/// The view table.
		/// </summary>
		public static readonly TableName View = new TableName(Name, "view");

		/// <summary>
		/// The NEW table used inside a triggered procedure to represent a triggered
		/// row after the operation occurs.
		/// </summary>
		public static readonly TableName NewTriggerTable = new TableName(Name, "NEW");

		/// <summary>
		/// The OLD table used inside a triggered procedure to represent a triggered
		/// row before the operation occurs.
		/// </summary>
		public static readonly TableName OldTriggerTable = new TableName(Name, "OLD");

		/// <summary>
		/// The table which provides information on the current connection
		/// </summary>
		public static readonly  TableName ConnectionInfo = new TableName(Name, "connection_info");

		/// <summary>
		/// The table which provides the list of all the active connections
		/// </summary>
		public static readonly TableName CurrentConnections = new TableName(Name, "current_connections");

		/// <summary>
		/// The table that provides information about the privileges assignable
		/// </summary>
		public static readonly TableName Privileges = new TableName(Name, "priv_map");

		public static readonly  TableName ProductInfo = new TableName(Name, "product_info");

		public static readonly TableName SqlTypes = new TableName(Name, "sql_types");

		public static readonly  TableName Variables = new TableName(Name, "variables");

		///<summary>
		///</summary>
		public static readonly TableName SchemaInfoTable = new TableName(Name, "schema_info");

		///<summary>
		///</summary>
		public static readonly TableName PersistentVarTable = new TableName(Name, "database_vars");

		///<summary>
		///</summary>
		public static readonly TableName ForeignColsTable = new TableName(Name, "foreign_columns");

		///<summary>
		///</summary>
		public static readonly TableName UniqueColsTable = new TableName(Name, "unique_columns");

		///<summary>
		///</summary>
		public static readonly TableName PrimaryColsTable = new TableName(Name, "primary_columns");

		///<summary>
		///</summary>
		public static readonly TableName CheckInfoTable = new TableName(Name, "check_info");

		///<summary>
		///</summary>
		public static readonly TableName UniqueInfoTable = new TableName(Name, "unique_info");

		///<summary>
		///</summary>
		public static readonly TableName ForeignInfoTable = new TableName(Name, "fkey_info");

		///<summary>
		///</summary>
		public static readonly TableName PrimaryInfoTable = new TableName(Name, "pkey_info");

		///<summary>
		///</summary>
		public static readonly TableName SysSequenceInfo = new TableName(Name, "sequence_info");

		///<summary>
		///</summary>
		public static readonly TableName SysSequence = new TableName(Name, "sequence");

		#region GT Tables DataTableInfo

		internal static readonly DataTableInfo TableColumnsTableInfo;

		internal static readonly DataTableInfo TableInfoTableInfo;

		internal static readonly DataTableInfo VariablesTableInfo;

		internal static readonly DataTableInfo ProductInfoTableInfo;

		internal static readonly DataTableInfo StatisticsTableInfo;

		internal static readonly DataTableInfo ConnectionInfoTableInfo;

		internal static readonly DataTableInfo CurrentConnectionsTableInfo;

		internal static readonly DataTableInfo PrivilegesTableInfo;

		internal static readonly DataTableInfo SqlTypesTableInfo;

		#endregion

		#region ..ctor

		static SystemSchema() {
			// TABLE_COLUMNS
			TableColumnsTableInfo = new DataTableInfo(TableColumns);
			TableColumnsTableInfo.AddColumn("schema", PrimitiveTypes.VarString);
			TableColumnsTableInfo.AddColumn("table", PrimitiveTypes.VarString);
			TableColumnsTableInfo.AddColumn("column", PrimitiveTypes.VarString);
			TableColumnsTableInfo.AddColumn("sql_type", PrimitiveTypes.Numeric);
			TableColumnsTableInfo.AddColumn("type_desc", PrimitiveTypes.VarString);
			TableColumnsTableInfo.AddColumn("size", PrimitiveTypes.Numeric);
			TableColumnsTableInfo.AddColumn("scale", PrimitiveTypes.Numeric);
			TableColumnsTableInfo.AddColumn("not_null", PrimitiveTypes.Boolean);
			TableColumnsTableInfo.AddColumn("default", PrimitiveTypes.VarString);
			TableColumnsTableInfo.AddColumn("index_str", PrimitiveTypes.VarString);
			TableColumnsTableInfo.AddColumn("seq_no", PrimitiveTypes.Numeric);
			TableColumnsTableInfo.IsReadOnly = true;

			// TABLE_INFO
			TableInfoTableInfo = new DataTableInfo(TableInfo);
			TableInfoTableInfo.AddColumn("schema", PrimitiveTypes.VarString);
			TableInfoTableInfo.AddColumn("name", PrimitiveTypes.VarString);
			TableInfoTableInfo.AddColumn("type", PrimitiveTypes.VarString);
			TableInfoTableInfo.AddColumn("other", PrimitiveTypes.VarString);
			TableInfoTableInfo.IsReadOnly = true;

			// VARIABLES
			VariablesTableInfo = new DataTableInfo(Variables);
			VariablesTableInfo.AddColumn("var", PrimitiveTypes.VarString);
			VariablesTableInfo.AddColumn("type", PrimitiveTypes.VarString);
			VariablesTableInfo.AddColumn("value", PrimitiveTypes.VarString);
			VariablesTableInfo.AddColumn("constant", PrimitiveTypes.Boolean);
			VariablesTableInfo.AddColumn("not_null", PrimitiveTypes.Boolean);
			VariablesTableInfo.AddColumn("is_set", PrimitiveTypes.Boolean);
			VariablesTableInfo.IsReadOnly = true;

			// PRODUCT_INFO
			ProductInfoTableInfo = new DataTableInfo(ProductInfo);
			ProductInfoTableInfo.AddColumn("var", PrimitiveTypes.VarString);
			ProductInfoTableInfo.AddColumn("value", PrimitiveTypes.VarString);
			ProductInfoTableInfo.IsReadOnly = true;

			// DATABASE_STATS
			StatisticsTableInfo = new DataTableInfo(DatabaseStatistics);
			StatisticsTableInfo.AddColumn("stat_name", PrimitiveTypes.VarString);
			StatisticsTableInfo.AddColumn("value", PrimitiveTypes.VarString);
			StatisticsTableInfo.IsReadOnly = true;

			// CONNECTION_INFO
			ConnectionInfoTableInfo = new DataTableInfo(ConnectionInfo);
			ConnectionInfoTableInfo.AddColumn("var", PrimitiveTypes.VarString);
			ConnectionInfoTableInfo.AddColumn("value", PrimitiveTypes.VarString);
			ConnectionInfoTableInfo.IsReadOnly = true;

			// CURRENT_CONNECTIONS
			CurrentConnectionsTableInfo = new DataTableInfo(CurrentConnections);
			CurrentConnectionsTableInfo.AddColumn("username", PrimitiveTypes.VarString);
			CurrentConnectionsTableInfo.AddColumn("host_string", PrimitiveTypes.VarString);
			CurrentConnectionsTableInfo.AddColumn("last_command", PrimitiveTypes.Date);
			CurrentConnectionsTableInfo.AddColumn("time_connected", PrimitiveTypes.Date);
			CurrentConnectionsTableInfo.IsReadOnly = true;

			// PRIVILEGES
			PrivilegesTableInfo = new DataTableInfo(SystemSchema.Privileges);
			PrivilegesTableInfo.AddColumn("priv_bit", PrimitiveTypes.Numeric);
			PrivilegesTableInfo.AddColumn("description", PrimitiveTypes.VarString);
			PrivilegesTableInfo.IsReadOnly = true;

			// SQL_TYPES
			SqlTypesTableInfo = new DataTableInfo(SqlTypes);
			SqlTypesTableInfo.AddColumn("TYPE_NAME", PrimitiveTypes.VarString);
			SqlTypesTableInfo.AddColumn("DATA_TYPE", PrimitiveTypes.Numeric);
			SqlTypesTableInfo.AddColumn("PRECISION", PrimitiveTypes.Numeric);
			SqlTypesTableInfo.AddColumn("LITERAL_PREFIX", PrimitiveTypes.VarString);
			SqlTypesTableInfo.AddColumn("LITERAL_SUFFIX", PrimitiveTypes.VarString);
			SqlTypesTableInfo.AddColumn("CREATE_PARAMS", PrimitiveTypes.VarString);
			SqlTypesTableInfo.AddColumn("NULLABLE", PrimitiveTypes.Numeric);
			SqlTypesTableInfo.AddColumn("CASE_SENSITIVE", PrimitiveTypes.Boolean);
			SqlTypesTableInfo.AddColumn("SEARCHABLE", PrimitiveTypes.Numeric);
			SqlTypesTableInfo.AddColumn("UNSIGNED_ATTRIBUTE", PrimitiveTypes.Boolean);
			SqlTypesTableInfo.AddColumn("FIXED_PREC_SCALE", PrimitiveTypes.Boolean);
			SqlTypesTableInfo.AddColumn("AUTO_INCREMENT", PrimitiveTypes.Boolean);
			SqlTypesTableInfo.AddColumn("LOCAL_TYPE_NAME", PrimitiveTypes.VarString);
			SqlTypesTableInfo.AddColumn("MINIMUM_SCALE", PrimitiveTypes.Numeric);
			SqlTypesTableInfo.AddColumn("MAXIMUM_SCALE", PrimitiveTypes.Numeric);
			SqlTypesTableInfo.AddColumn("SQL_DATA_TYPE", PrimitiveTypes.VarString);
			SqlTypesTableInfo.AddColumn("SQL_DATETIME_SUB", PrimitiveTypes.VarString);
			SqlTypesTableInfo.AddColumn("NUM_PREC_RADIX", PrimitiveTypes.Numeric);
			SqlTypesTableInfo.IsReadOnly = true;
		}

		#endregion

		internal static void AddSystemTables(ICommitableTransaction transaction) {
			// SYSTEM.SEQUENCE_INFO
			DataTableInfo tableInfo = new DataTableInfo(SysSequenceInfo);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("schema", PrimitiveTypes.VarString);
			tableInfo.AddColumn("name", PrimitiveTypes.VarString);
			tableInfo.AddColumn("type", PrimitiveTypes.Numeric);
			transaction.AlterCreateTable(tableInfo, 187, 128);

			// SYSTEM.SEQUENCE
			tableInfo = new DataTableInfo(SysSequence);
			tableInfo.AddColumn("seq_id", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("last_value", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("increment", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("minvalue", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("maxvalue", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("start", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("cache", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("cycle", PrimitiveTypes.Boolean);
			transaction.AlterCreateTable(tableInfo, 187, 128);

			// SYSTEM.PRIMARY_INFO
			tableInfo = new DataTableInfo(PrimaryInfoTable);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("name", PrimitiveTypes.VarString);
			tableInfo.AddColumn("schema", PrimitiveTypes.VarString);
			tableInfo.AddColumn("table", PrimitiveTypes.VarString);
			tableInfo.AddColumn("deferred", PrimitiveTypes.Numeric);
			transaction.AlterCreateTable(tableInfo, 187, 128);

			tableInfo = new DataTableInfo(ForeignInfoTable);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("name", PrimitiveTypes.VarString);
			tableInfo.AddColumn("schema", PrimitiveTypes.VarString);
			tableInfo.AddColumn("table", PrimitiveTypes.VarString);
			tableInfo.AddColumn("ref_schema", PrimitiveTypes.VarString);
			tableInfo.AddColumn("ref_table", PrimitiveTypes.VarString);
			tableInfo.AddColumn("update_rule", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("delete_rule", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("deferred", PrimitiveTypes.Numeric);
			transaction.AlterCreateTable(tableInfo, 187, 128);

			tableInfo = new DataTableInfo(UniqueInfoTable);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("name", PrimitiveTypes.VarString);
			tableInfo.AddColumn("schema", PrimitiveTypes.VarString);
			tableInfo.AddColumn("table", PrimitiveTypes.VarString);
			tableInfo.AddColumn("deferred", PrimitiveTypes.Numeric);
			transaction.AlterCreateTable(tableInfo, 187, 128);

			tableInfo = new DataTableInfo(CheckInfoTable);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("name", PrimitiveTypes.VarString);
			tableInfo.AddColumn("schema", PrimitiveTypes.VarString);
			tableInfo.AddColumn("table", PrimitiveTypes.VarString);
			tableInfo.AddColumn("expression", PrimitiveTypes.VarString);
			tableInfo.AddColumn("deferred", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("serialized_expression", PrimitiveTypes.BinaryType);
			transaction.AlterCreateTable(tableInfo, 187, 128);

			tableInfo = new DataTableInfo(PrimaryColsTable);
			tableInfo.AddColumn("pk_id", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("column", PrimitiveTypes.VarString);
			tableInfo.AddColumn("seq_no", PrimitiveTypes.Numeric);
			transaction.AlterCreateTable(tableInfo, 91, 128);

			tableInfo = new DataTableInfo(UniqueColsTable);
			tableInfo.AddColumn("un_id", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("column", PrimitiveTypes.VarString);
			tableInfo.AddColumn("seq_no", PrimitiveTypes.Numeric);
			transaction.AlterCreateTable(tableInfo, 91, 128);

			tableInfo = new DataTableInfo(ForeignColsTable);
			tableInfo.AddColumn("fk_id", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("fcolumn", PrimitiveTypes.VarString);
			tableInfo.AddColumn("pcolumn", PrimitiveTypes.VarString);
			tableInfo.AddColumn("seq_no", PrimitiveTypes.Numeric);
			transaction.AlterCreateTable(tableInfo, 91, 128);

			tableInfo = new DataTableInfo(SchemaInfoTable);
			tableInfo.AddColumn("id", PrimitiveTypes.Numeric);
			tableInfo.AddColumn("name", PrimitiveTypes.VarString);
			tableInfo.AddColumn("type", PrimitiveTypes.VarString);
			tableInfo.AddColumn("other", PrimitiveTypes.VarString);
			transaction.AlterCreateTable(tableInfo, 91, 128);

			// Stores misc variables of the database,
			tableInfo = new DataTableInfo(PersistentVarTable);
			tableInfo.AddColumn("variable", PrimitiveTypes.VarString);
			tableInfo.AddColumn("value", PrimitiveTypes.VarString);
			transaction.AlterCreateTable(tableInfo, 91, 128);
		}

		internal static void Initialize(ICommitableTransaction transaction) {
			// -- Primary Keys --
			// The 'id' columns are primary keys on all the system tables,
			var idCol = new String[] { "id" };
			transaction.AddPrimaryKeyConstraint(PrimaryInfoTable, idCol, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_PK_PK");
			transaction.AddPrimaryKeyConstraint(ForeignInfoTable, idCol, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_FK_PK");
			transaction.AddPrimaryKeyConstraint(UniqueInfoTable, idCol, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_UNIQUE_PK");
			transaction.AddPrimaryKeyConstraint(CheckInfoTable, idCol, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_CHECK_PK");
			transaction.AddPrimaryKeyConstraint(SchemaInfoTable, idCol, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_SCHEMA_PK");

			// -- Foreign Keys --
			// Create the foreign key references,
			var fkCol = new String[1];
			var fkRefCol = new[] { "id" };

			fkCol[0] = "pk_id";
			transaction.AddForeignKeyConstraint(PrimaryColsTable,
				fkCol,
				PrimaryInfoTable,
				fkRefCol,
				ConstraintAction.NoAction,
				ConstraintAction.NoAction,
				ConstraintDeferrability.InitiallyImmediate,
				"SYSTEM_PK_FK");

			fkCol[0] = "fk_id";
			transaction.AddForeignKeyConstraint(ForeignColsTable,
				fkCol,
				ForeignInfoTable,
				fkRefCol,
				ConstraintAction.NoAction,
				ConstraintAction.NoAction,
				ConstraintDeferrability.InitiallyImmediate,
				"SYSTEM_FK_FK");

			fkCol[0] = "un_id";
			transaction.AddForeignKeyConstraint(UniqueColsTable,
				fkCol,
				UniqueInfoTable,
				fkRefCol,
				ConstraintAction.NoAction,
				ConstraintAction.NoAction,
				ConstraintDeferrability.InitiallyImmediate,
				"SYSTEM_UNIQUE_FK");

			// pkey_info 'schema', 'table' column is a unique set,
			// (You are only allowed one primary key per table).
			String[] columns = new String[] { "schema", "table" };
			transaction.AddUniqueConstraint(PrimaryInfoTable, columns, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_PKEY_ST_UNIQUE");

			// schema_info 'name' column is a unique column,
			columns = new String[] { "name" };
			transaction.AddUniqueConstraint(SchemaInfoTable, columns, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_SCHEMA_UNIQUE");

			//    columns = new String[] { "name" };
			columns = new String[] { "name", "schema" };
			// pkey_info 'name' column is a unique column,
			transaction.AddUniqueConstraint(PrimaryInfoTable, columns, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_PKEY_UNIQUE");

			// fkey_info 'name' column is a unique column,
			transaction.AddUniqueConstraint(ForeignInfoTable, columns, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_FKEY_UNIQUE");

			// unique_info 'name' column is a unique column,
			transaction.AddUniqueConstraint(UniqueInfoTable, columns, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_UNIQUE_UNIQUE");

			// check_info 'name' column is a unique column,
			transaction.AddUniqueConstraint(CheckInfoTable, columns, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_CHECK_UNIQUE");

			// database_vars 'variable' is unique
			columns = new String[] { "variable" };
			transaction.AddUniqueConstraint(PersistentVarTable, columns, ConstraintDeferrability.InitiallyImmediate, "SYSTEM_DATABASEVARS_UNIQUE");
		}

		internal static void CreateTables(DatabaseConnection connection) {
			// --- The user management tables ---
			var password = new DataTableInfo(Password);
			password.AddColumn("UserName", PrimitiveTypes.VarString);
			password.AddColumn("Password", PrimitiveTypes.VarString);
			password.AddColumn("Salt", PrimitiveTypes.VarString);
			password.AddColumn("Hash", PrimitiveTypes.VarString);

			var userPriv = new DataTableInfo(UserPrivileges);
			userPriv.AddColumn("UserName", PrimitiveTypes.VarString);
			userPriv.AddColumn("PrivGroupName", PrimitiveTypes.VarString);

			var userConnectPriv = new DataTableInfo(UserConnectPrivileges);
			userConnectPriv.AddColumn("UserName", PrimitiveTypes.VarString);
			userConnectPriv.AddColumn("Protocol", PrimitiveTypes.VarString);
			userConnectPriv.AddColumn("Host", PrimitiveTypes.VarString);
			userConnectPriv.AddColumn("Access", PrimitiveTypes.VarString);

			var grant = new DataTableInfo(Grant);
			grant.AddColumn("priv_bit", PrimitiveTypes.Numeric);
			grant.AddColumn("object", PrimitiveTypes.Numeric);
			grant.AddColumn("param", PrimitiveTypes.VarString);
			grant.AddColumn("grantee", PrimitiveTypes.VarString);
			grant.AddColumn("grant_option", PrimitiveTypes.VarString);
			grant.AddColumn("granter", PrimitiveTypes.VarString);

			var service = new DataTableInfo(Service);
			service.AddColumn("name", PrimitiveTypes.VarString);
			service.AddColumn("class", PrimitiveTypes.VarString);
			service.AddColumn("type", PrimitiveTypes.VarString);

			var functionFactory = new DataTableInfo(Functionfactory);
			functionFactory.AddColumn("name", PrimitiveTypes.VarString);
			functionFactory.AddColumn("class", PrimitiveTypes.VarString);
			functionFactory.AddColumn("type", PrimitiveTypes.VarString);

			var function = new DataTableInfo(Function);
			function.AddColumn("schema", PrimitiveTypes.VarString);
			function.AddColumn("name", PrimitiveTypes.VarString);
			function.AddColumn("type", PrimitiveTypes.VarString);
			function.AddColumn("location", PrimitiveTypes.VarString);
			function.AddColumn("return_type", PrimitiveTypes.VarString);
			function.AddColumn("args_type", PrimitiveTypes.VarString);
			function.AddColumn("username", PrimitiveTypes.VarString);

			var view = new DataTableInfo(View);
			view.AddColumn("schema", PrimitiveTypes.VarString);
			view.AddColumn("name", PrimitiveTypes.VarString);
			view.AddColumn("query", PrimitiveTypes.BinaryType);
			view.AddColumn("data", PrimitiveTypes.BinaryType);
			view.AddColumn("username", PrimitiveTypes.VarString);

			var label = new DataTableInfo(Label);
			label.AddColumn("object_type", PrimitiveTypes.Numeric);
			label.AddColumn("object_name", PrimitiveTypes.VarString);
			label.AddColumn("label", PrimitiveTypes.VarString);

			var dataTrigger = new DataTableInfo(DataTrigger);
			dataTrigger.AddColumn("schema", PrimitiveTypes.VarString);
			dataTrigger.AddColumn("name", PrimitiveTypes.VarString);
			dataTrigger.AddColumn("type", PrimitiveTypes.Numeric);
			dataTrigger.AddColumn("on_object", PrimitiveTypes.VarString);
			dataTrigger.AddColumn("action", PrimitiveTypes.VarString);
			dataTrigger.AddColumn("misc", PrimitiveTypes.BinaryType);
			dataTrigger.AddColumn("username", PrimitiveTypes.VarString);

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

		internal static void SetTableGrants(GrantManager manager, string granter) {
			// Set the public grants for the system tables,
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, ConnectionInfo.ToString(), User.PublicName, false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, CurrentConnections.ToString(), User.PublicName, false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, Variables.ToString(), User.PublicName, false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, DatabaseStatistics.ToString(), User.PublicName, false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, Variables.ToString(), User.PublicName, false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, ProductInfo.ToString(), User.PublicName, false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, SqlTypes.ToString(), User.PublicName, false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, Function.ToString(), User.PublicName, false, granter);
		}

		///<summary>
		/// Sets all the standard functions and procedures available to engine.
		///</summary>
		///<param name="connection"></param>
		///<param name="adminUser"></param>
		/// <remarks>
		/// This creates an entry in the SysFunction table for all the dynamic
		/// functions and procedures.  This may not include the functions exposed
		/// though the FunctionFactory interface.
		/// </remarks>
		internal static void SetupSystemFunctions(DatabaseConnection connection, string adminUser) {
			// TODO: here we should load all the system functions
			
			const String granter = User.SystemName;

			// The manager handling the functions.
			RoutinesManager manager = connection.RoutinesManager;

			// Define the SYSTEM_MAKE_BACKUP procedure
			manager.DefineExternalRoutine(new RoutineName(Name, "SYSTEM_MAKE_BACKUP"),
				ExternalRoutineInfo.FormatString(typeof(SystemBackup)),
				null,
				new TType[] {PrimitiveTypes.VarString},
				adminUser);

			// -----

			// Set the grants for the procedures.
			GrantManager grants = connection.GrantManager;

			// Revoke all existing grants on the internal stored procedures.
			grants.RevokeAllGrantsOnObject(GrantObject.Table, "SYSTEM.SYSTEM_MAKE_BACKUP");

			// Grant execute priv with grant option to administrator
			grants.Grant(Security.Privileges.ProcedureExecute, GrantObject.Table, "SYSTEM.SYSTEM_MAKE_BACKUP", adminUser, true, granter);
		}

		public static ITableDataSource GetTableColumnsTable(SimpleTransaction transaction) {
			return new GTTableColumnsDataSource(transaction);
		}

		internal static ITableDataSource GetTableInfoTable(Transaction transaction) {
			return new GTTableInfoDataSource(transaction);
		}

		internal static ITableDataSource GetVariablesTable(Transaction transaction) {
			return new GTVariablesDataSource(transaction);
		}

		internal static ITableDataSource GetProductInfoTable(Transaction transaction) {
			return new GTProductDataSource(transaction);
		}

		internal static ITableDataSource GetStatisticsTable(DatabaseConnection connection) {
			return new GTStatisticsDataSource(connection);
		}

		internal static ITableDataSource GetConnectionInfoTable(DatabaseConnection connection) {
			return new ConnectionInfoDataSource(connection);
		}

		internal static ITableDataSource GetCurrentConnectionsTable(DatabaseConnection connection) {
			return new CurrentConnectionsDataSource(connection);
		}

		internal static ITableDataSource GetPrivilegesTable(DatabaseConnection connection) {
			return new PrivilegesDataSource(connection);
		}

		internal static ITableDataSource GetSqlTypesTable(DatabaseConnection connection) {
			return new SqlTypesDataSource(connection);
		}

		#region GTTableColumnsDataSource

		/// <summary>
		/// An implementation of <see cref="ITableDataSource"/> that 
		/// presents information about the columns of all tables in all schema.
		/// </summary>
		/// <remarks>
		/// <b>Note</b> This is not designed to be a long kept object. It must not last
		/// beyond the lifetime of a transaction.
		/// </remarks>
		sealed class GTTableColumnsDataSource : GTDataSource {
			/// <summary>
			/// The transaction that is the view of this information.
			/// </summary>
			private SimpleTransaction transaction;
			/// <summary>
			/// The list of all DataTableInfo visible to the transaction.
			/// </summary>
			private DataTableInfo[] visibleTables;
			/// <summary>
			/// The number of rows in this table.
			/// </summary>
			private int rowCount;

			public GTTableColumnsDataSource(SimpleTransaction transaction)
				: base(transaction.Context.SystemContext) {
				this.transaction = transaction;
				Init();
			}

			/// <summary>
			/// Initialize the data source.
			/// </summary>
			/// <returns></returns>
			private void Init() {
				// All the tables
				TableName[] list = transaction.GetTables();
				visibleTables = new DataTableInfo[list.Length];
				rowCount = 0;
				for (int i = 0; i < list.Length; ++i) {
					DataTableInfo info = transaction.GetTableInfo(list[i]);
					rowCount += info.ColumnCount;
					visibleTables[i] = info;
				}
			}

			// ---------- Implemented from GTDataSource ----------

			public override DataTableInfo TableInfo {
				get { return TableColumnsTableInfo; }
			}

			public override int RowCount {
				get { return rowCount; }
			}

			public override TObject GetCell(int column, int row) {

				int sz = visibleTables.Length;
				int rs = 0;
				for (int n = 0; n < sz; ++n) {
					DataTableInfo info = visibleTables[n];
					int b = rs;
					rs += info.ColumnCount;
					if (row >= b && row < rs) {
						// This is the column that was requested,
						int seqNo = row - b;
						DataColumnInfo colInfo = info[seqNo];
						switch (column) {
							case 0:  // schema
								return GetColumnValue(column, info.Schema);
							case 1:  // table
								return GetColumnValue(column, info.Name);
							case 2:  // column
								return GetColumnValue(column, colInfo.Name);
							case 3:  // sql_type
								return GetColumnValue(column, (BigNumber)(int)colInfo.SqlType);
							case 4:  // type_desc
								return GetColumnValue(column, colInfo.TType.ToSqlString());
							case 5:  // size
								return GetColumnValue(column, (BigNumber)colInfo.Size);
							case 6:  // scale
								return GetColumnValue(column, (BigNumber)colInfo.Scale);
							case 7:  // not_null
								return GetColumnValue(column, colInfo.IsNotNull);
							case 8:  // default
								return GetColumnValue(column, colInfo.GetDefaultExpressionString());
							case 9:  // index_str
								return GetColumnValue(column, colInfo.IndexScheme);
							case 10:  // seq_no
								return GetColumnValue(column, (BigNumber)seqNo);
							default:
								throw new ApplicationException("Column out of bounds.");
						}
					}

				}  // for each visible table

				throw new ApplicationException("Row out of bounds.");
			}

			// ---------- Overwritten ----------

			protected override void Dispose(bool disposing) {
				visibleTables = null;
				transaction = null;
			}
		}

		#endregion

		#region GTTableInfoDataSource

		/// <summary>
		/// An implementation of <see cref="IMutableTableDataSource"/> that 
		/// presents information about the tables in all schema.
		/// </summary>
		/// <remarks>
		/// <b>Note</b> This is not designed to be a long kept object. It must not 
		/// last beyond the lifetime of a transaction.
		/// </remarks>
		sealed class GTTableInfoDataSource : GTDataSource {
			/// <summary>
			/// The transaction that is the view of this information.
			/// </summary>
			private Transaction transaction;

			private List<TTableInfo> tableInfos;

			/// <summary>
			/// The number of rows in this table.
			/// </summary>
			private int rowCount;

			public GTTableInfoDataSource(Transaction transaction)
				: base(transaction.Context.SystemContext) {
				this.transaction = transaction;
				tableInfos = new List<TTableInfo>();
				Init();
			}

			/// <summary>
			/// Initialize the data source.
			/// </summary>
			/// <returns></returns>
			private void Init() {
				// All the tables
				TableName[] tableList = transaction.GetTables();
				Array.Sort(tableList);
				rowCount = tableList.Length;

				foreach (TableName tableName in tableList) {
					string curType = transaction.GetTableType(tableName);

					// If the table is in the SYSTEM schema, the type is defined as a
					// SYSTEM TABLE.
					if (curType.Equals("TABLE") &&
						tableName.Schema.Equals("SYSTEM")) {
						curType = "SYSTEM TABLE";
					}

					TTableInfo tableInfo = new TTableInfo();
					tableInfo.Name = tableName.Name;
					tableInfo.Schema = tableName.Schema;
					tableInfo.Type = curType;

					tableInfos.Add(tableInfo);
				}
			}

			// ---------- Implemented from GTDataSource ----------

			public override DataTableInfo TableInfo {
				get { return TableInfoTableInfo; }
			}

			public override int RowCount {
				get { return rowCount; }
			}

			public override TObject GetCell(int column, int row) {
				TTableInfo info = tableInfos[row];
				switch (column) {
					case 0:  // schema
						return GetColumnValue(column, info.Schema);
					case 1:  // name
						return GetColumnValue(column, info.Name);
					case 2:  // type
						return GetColumnValue(column, info.Type);
					case 3:  // other
						// Table notes, etc.  (future enhancement)
						return GetColumnValue(column, info.Notes);
					default:
						throw new ApplicationException("Column out of bounds.");
				}
			}

			// ---------- Overwritten from GTDataSource ----------

			protected override void Dispose(bool disposing) {
				tableInfos = null;
				transaction = null;
			}

			#region TableInfo

			class TTableInfo {
				public string Name;
				public string Schema;
				public string Type;
				public string Notes;

			}

			#endregion
		}

		#endregion

		#region GTVariablesDataSource

		class GTVariablesDataSource : GTDataSource {
			public GTVariablesDataSource(SimpleTransaction transaction)
				: base(transaction.Context.SystemContext) {
				this.transaction = transaction;
				variables = new List<VariableInfo>();
				Init();
			}

			private SimpleTransaction transaction;

			/// <summary>
			/// The list of info keys/values in this object.
			/// </summary>
			private List<VariableInfo> variables;

			public override DataTableInfo TableInfo {
				get { return VariablesTableInfo; }
			}

			public override int RowCount {
				get { return variables.Count / 4; }
			}

			public override TObject GetCell(int column, int row) {
				VariableInfo variable = variables[row];

				switch (column) {
					case 0:  // var
						return GetColumnValue(column, variable.Name);
					case 1:  // type
						return GetColumnValue(column, variable.SqlType);
					case 2:  // value
						return GetColumnValue(column, variable.Value);
					case 3:  // constant
						return GetColumnValue(column, variable.IsConstant);
					case 4:  // not_null
						return GetColumnValue(column, variable.IsNotNull);
					case 5:  // is_set
						return GetColumnValue(column, variable.IsSet);
					default:
						throw new ApplicationException("Column out of bounds.");
				}
			}

			public void Init() {
				VariablesManager variablesManager = transaction.Variables;
				lock (variablesManager) {
					for (int i = 0; i < variablesManager.Count; i++) {
						Variable variable = variablesManager[i];
						variables.Add(new VariableInfo(variable));
					}
				}
			}

			protected override void Dispose(bool disposing) {
				variables = null;
				transaction = null;
			}

			#region DbVariable

			class VariableInfo {
				public VariableInfo(Variable variable) {
					Name = variable.Name;
					SqlType = variable.Type.ToSqlString();
					Value = variable.IsSet ? variable.original_expression.Text.ToString() : "NULL";
					IsConstant = variable.Constant;
					IsNotNull = variable.NotNull;
					IsSet = variable.IsSet;
				}

				public readonly string Name;
				public readonly string SqlType;
				public readonly string Value;
				public readonly bool IsConstant;
				public readonly bool IsNotNull;
				public readonly bool IsSet;
			}

			#endregion
		}

		#endregion

		#region GTProductDataSource

		/// <summary>
		/// An implementation of <see cref="IMutableTableDataSource"/> that models 
		/// information about the software.
		/// </summary>
		/// <remarks>
		/// <b>Note:</b> This is not designed to be a long kept object. It must not last
		/// beyond the lifetime of a transaction.
		/// </remarks>
		sealed class GTProductDataSource : GTDataSource {
			/// <summary>
			/// The list of info keys/values in this object.
			/// </summary>
			private List<string> keyValuePairs;

			public GTProductDataSource(SimpleTransaction transaction)
				: base(transaction.Context.SystemContext) {
				keyValuePairs = new List<string>();
				Init();
			}

			/// <summary>
			/// Initialize the data source.
			/// </summary>
			/// <returns></returns>
			private void Init() {
				ProductInfo productInfo = Util.ProductInfo.Current;
				// Set up the product variables.
				keyValuePairs.Add("title");
				keyValuePairs.Add(productInfo.Title);

				keyValuePairs.Add("version");
				keyValuePairs.Add(productInfo.Version.ToString());

				keyValuePairs.Add("copyright");
				keyValuePairs.Add(productInfo.Copyright);

				keyValuePairs.Add("description");
				keyValuePairs.Add(productInfo.Description);

				keyValuePairs.Add("company");
				keyValuePairs.Add(productInfo.Company);
			}

			// ---------- Implemented from GTDataSource ----------

			public override DataTableInfo TableInfo {
				get { return ProductInfoTableInfo; }
			}

			public override int RowCount {
				get { return keyValuePairs.Count / 2; }
			}

			public override TObject GetCell(int column, int row) {
				switch (column) {
					case 0:  // var
						return GetColumnValue(column, keyValuePairs[row * 2]);
					case 1:  // value
						return GetColumnValue(column, keyValuePairs[(row * 2) + 1]);
					default:
						throw new ApplicationException("Column out of bounds.");
				}
			}

			// ---------- Overwritten from GTDataSource ----------

			protected override void Dispose(bool disposing) {
				keyValuePairs = null;
			}
		}

		#endregion

		#region GTStatisticsDataSource

		/// <summary>
		/// An implementation of <see cref="IMutableTableDataSource"/> that 
		/// presents database statistical information.
		/// </summary>
		/// <remarks>
		/// <b>Note:</b> This is not designed to be a long kept object. It must not last
		/// beyond the lifetime of a transaction.
		/// </remarks>
		sealed class GTStatisticsDataSource : GTDataSource {
			/// <summary>
			/// Contains all the statistics information for this session.
			/// </summary>
			private string[] statsInfo;
			/// <summary>
			/// The system database stats.
			/// </summary>
			private Stats stats;

			public GTStatisticsDataSource(DatabaseConnection connection)
				: base(connection.Context) {
				stats = connection.Database.Context.Stats;
				Init();
			}

			/// <summary>
			/// Initialize the data source.
			/// </summary>
			/// <returns></returns>
			private void Init() {
				lock (stats) {
					// TODO: get the value of the db_path drive
					var driveInfo = DriveInfo.GetDrives()[0];

					stats.Set("Runtime.Memory.FreeSpaceKb", (int)driveInfo.AvailableFreeSpace / 1024);
					stats.Set("Runtime.Memory.TotalFreeSpaceKb", (int)driveInfo.TotalFreeSpace / 1024);
					stats.Set("Runtime.Memory.TotalKb", (int)driveInfo.TotalSize / 1024);

					string[] keySet = stats.Keys;
					int globLength = keySet.Length * 2;
					statsInfo = new string[globLength];

					for (int i = 0; i < globLength; i += 2) {
						string keyName = keySet[i / 2];
						statsInfo[i] = keyName;
						statsInfo[i + 1] = stats.StatString(keyName);
					}

				}
			}

			// ---------- Implemented from GTDataSource ----------

			public override DataTableInfo TableInfo {
				get { return StatisticsTableInfo; }
			}

			public override int RowCount {
				get { return statsInfo.Length / 2; }
			}

			public override TObject GetCell(int column, int row) {
				switch (column) {
					case 0:  // stat_name
						return GetColumnValue(column, statsInfo[row * 2]);
					case 1:  // value
						return GetColumnValue(column, statsInfo[(row * 2) + 1]);
					default:
						throw new ApplicationException("Column out of bounds.");
				}
			}

			// ---------- Overwritten from GTDataSource ----------

			protected override void Dispose(bool disposing) {
				statsInfo = null;
				stats = null;
			}
		}

		#endregion

		#region ConnectionInfoDataSource

		/// <summary>
		/// An implementation of <see cref="IMutableTableDataSource"/> that 
		/// presents the current session information.
		/// </summary>
		/// <remarks>
		/// <b>Note:</b> This is not designed to be a long kept object. 
		/// It must not last beyond the lifetime of a transaction.
		/// </remarks>
		sealed class ConnectionInfoDataSource : GTDataSource {
			/// <summary>
			/// The DatabaseConnection object that this is table is modelling the
			/// information within.
			/// </summary>
			private DatabaseConnection database;

			/// <summary>
			/// The list of info keys/values in this object.
			/// </summary>
			private List<string> keyValuePairs;

			public ConnectionInfoDataSource(DatabaseConnection connection)
				: base(connection.Context) {
				database = connection;
				keyValuePairs = new List<string>();
				Init();
			}

			/// <summary>
			/// Initialize the data source.
			/// </summary>
			/// <returns></returns>
			private void Init() {
				// Set up the connection info variables.
				keyValuePairs.Add("auto_commit");
				keyValuePairs.Add(database.AutoCommit ? "true" : "false");

				keyValuePairs.Add("isolation_level");
				keyValuePairs.Add(database.TransactionIsolation.ToString());

				keyValuePairs.Add("user");
				keyValuePairs.Add(database.User.UserName);

				keyValuePairs.Add("time_connection");
				keyValuePairs.Add(database.User.TimeConnected.ToString());

				keyValuePairs.Add("connection_string");
				keyValuePairs.Add(database.User.ConnectionString);

				keyValuePairs.Add("current_schema");
				keyValuePairs.Add(database.CurrentSchema);

				keyValuePairs.Add("case_insensitive_identifiers");
				keyValuePairs.Add(database.IsInCaseInsensitiveMode ? "true" : "false");
			}

			// ---------- Implemented from GTDataSource ----------

			/// <inheritdoc/>
			public override DataTableInfo TableInfo {
				get { return ConnectionInfoTableInfo; }
			}

			/// <inheritdoc/>
			public override int RowCount {
				get { return keyValuePairs.Count / 2; }
			}

			/// <inheritdoc/>
			public override TObject GetCell(int column, int row) {
				switch (column) {
					case 0:  // var
						return GetColumnValue(column, keyValuePairs[row * 2]);
					case 1:  // value
						return GetColumnValue(column, keyValuePairs[(row * 2) + 1]);
					default:
						throw new ApplicationException("Column out of bounds.");
				}
			}

			// ---------- Overwritten from GTDataSource ----------

			/// <inheritdoc/>
			protected override void Dispose(bool disposing) {
				if (disposing) {
					keyValuePairs = null;
					database = null;
				}
			}
		}

		#endregion

		#region CurrentConnectionsDataSource

		/// <summary>
		/// An implementation of <see cref="IMutableTableDataSource"/> that 
		/// presents the current list of sessions on the database.
		/// </summary>
		/// <remarks>
		/// <b>Note:</b> This is not designed to be a long kept object. 
		/// It must not last beyond the lifetime of a transaction.
		/// </remarks>
		sealed class CurrentConnectionsDataSource : GTDataSource {
			/// <summary>
			/// The DatabaseConnection object that this is table is modelling the
			/// information within.
			/// </summary>
			private DatabaseConnection database;

			/// <summary>
			/// The list of info keys/values in this object.
			/// </summary>
			private List<CurrentConnection> connections;

			public CurrentConnectionsDataSource(DatabaseConnection connection)
				: base(connection.Context) {
				database = connection;
				connections = new List<CurrentConnection>();
				Init();
			}

			/// <summary>
			/// Initialize the data source.
			/// </summary>
			/// <returns></returns>
			private void Init() {
				LoggedUsers loggedUsers = database.Database.Context.LoggedUsers;

				// Synchronize over the user manager while we inspect the information,
				lock (loggedUsers) {
					for (int i = 0; i < loggedUsers.UserCount; ++i) {
						User user = loggedUsers[i];
						CurrentConnection currentConnection = new CurrentConnection();
						currentConnection.UserName = user.UserName;
						currentConnection.Host = user.ConnectionString;
						currentConnection.LastCommand = user.LastCommandTime;
						currentConnection.Connected = user.TimeConnected;

						connections.Add(currentConnection);
					}
				}
			}

			// ---------- Implemented from GTDataSource ----------

			/// <inheritdoc/>
			public override DataTableInfo TableInfo {
				get { return CurrentConnectionsTableInfo; }
			}

			/// <inheritdoc/>
			public override int RowCount {
				get { return connections.Count / 4; }
			}

			/// <inheritdoc/>
			public override TObject GetCell(int column, int row) {
				CurrentConnection currentConnection = connections[row];

				switch (column) {
					case 0:  // username
						return GetColumnValue(column, currentConnection.UserName);
					case 1:  // host_string
						return GetColumnValue(column, currentConnection.Host);
					case 2:  // last_command
						return GetColumnValue(column, currentConnection.LastCommand);
					case 3:  // time_connected
						return GetColumnValue(column, currentConnection.Connected);
					default:
						throw new IndexOutOfRangeException();
				}
			}

			// ---------- Overwritten from GTDataSource ----------

			/// <inheritdoc/>
			protected override void Dispose(bool disposing) {
				if (disposing) {
					connections = null;
					database = null;
				}
			}

			#region CurrentConnection

			class CurrentConnection {
				public string UserName;
				public string Host;
				public DateTime LastCommand;
				public DateTime Connected;
			}

			#endregion
		}


		#endregion

		#region PrivilegesDataSource

		/// <summary>
		/// A <see cref="GTDataSource"/> that maps a 11-bit <see cref="Privileges"/> 
		/// to strings that represent the privilege in human readable string.
		/// </summary>
		/// <remarks>
		/// Each 11-bit priv set contains 12 entries for each bit that was set.
		/// <para>
		/// This table provides a convenient way to join the system grant table and
		/// <i>expand</i> the privileges that are allowed though it.
		/// </para>
		/// </remarks>
		class PrivilegesDataSource : GTDataSource {
			/// <summary>
			/// Number of bits.
			/// </summary>
			private const int BitCount = Security.Privileges.BitCount;

			public PrivilegesDataSource(DatabaseConnection connection)
				: base(connection.Context) {
			}

			// ---------- Implemented from GTDataSource ----------

			public override DataTableInfo TableInfo {
				get { return PrivilegesTableInfo; }
			}

			public override int RowCount {
				get { return (1 << BitCount) * BitCount; }
			}

			public override TObject GetCell(int column, int row) {
				int c1 = row / BitCount;
				if (column == 0)
					return GetColumnValue(column, (BigNumber)c1);

				int privBit = (1 << (row % BitCount));
				string privString = null;
				if ((c1 & privBit) != 0) {
					privString = Security.Privileges.FormatPriv(privBit);
				}
				return GetColumnValue(column, privString);
			}


			public override SelectableScheme GetColumnScheme(int column) {
				if (column == 0)
					return new PrivMapSearch(this, column);
				return new BlindSearch(this, column);
			}

			// ---------- Inner classes ----------

			/// <summary>
			/// A SelectableScheme that makes searching on the 'priv_bit' column 
			/// a lot less painless!
			/// </summary>
			private sealed class PrivMapSearch : CollatedBaseSearch {

				internal PrivMapSearch(ITableDataSource table, int column)
					: base(table, column) {
				}

				public override SelectableScheme Copy(ITableDataSource table, bool immutable) {
					// Return a fresh object.  This implementation has no state so we can
					// ignore the 'immutable' flag.
					return new BlindSearch(table, Column);
				}

				protected override int SearchFirst(TObject val) {
					if (val.IsNull) {
						return -1;
					}

					int num = ((BigNumber)val.Object).ToInt32();

					if (num < 0)
						return -1;
					if (num > (1 << BitCount))
						return -(((1 << BitCount) * BitCount) + 1);

					return (num * BitCount);
				}

				protected override int SearchLast(TObject val) {
					int p = SearchFirst(val);
					if (p >= 0)
						return p + (BitCount - 1);
					return p;
				}
			}
		}

		#endregion

		#region SqlTypesDataSource

		/// <summary>
		/// A <see cref="GTDataSource"/> that models all SQL types available.
		/// </summary>
		/// <remarks>
		/// <b>Note:</b> This is not designed to be a long kept object. It must 
		/// not last beyond the lifetime of a transaction.
		/// </remarks>
		class SqlTypesDataSource : GTDataSource {
			/// <summary>
			/// The DatabaseConnection object.  Currently this is not used, but it may
			/// be needed in the future if user-defined SQL types are supported.
			/// </summary>
			private DatabaseConnection database;

			/// <summary>
			/// The list of info keys/values in this object.
			/// </summary>
			private List<SqlTypeInfo> sqlTypes;

			/// <summary>
			/// Constant for type_nullable types.
			/// </summary>
			private static readonly BigNumber TypeNullable = 1;

			public SqlTypesDataSource(DatabaseConnection connection)
				: base(connection.Context) {
				database = connection;
				sqlTypes = new List<SqlTypeInfo>();
				Init();
			}

			/// <summary>
			/// Adds a type description.
			/// </summary>
			/// <param name="name"></param>
			/// <param name="type"></param>
			/// <param name="precision"></param>
			/// <param name="prefix"></param>
			/// <param name="suffix"></param>
			/// <param name="searchable"></param>
			private void AddType(string name, SqlType type, byte precision, string prefix, string suffix, bool searchable) {
				SqlTypeInfo typeInfo = new SqlTypeInfo();
				typeInfo.Name = name;
				typeInfo.Type = type;
				typeInfo.Precision = precision;
				typeInfo.LiteralPrefix = prefix;
				typeInfo.LiteralSuffix = suffix;
				typeInfo.Searchable = (byte)(searchable ? 3 : 0);
				sqlTypes.Add(typeInfo);
			}

			/// <summary>
			/// Initialize the data source.
			/// </summary>
			/// <returns></returns>
			private void Init() {
				AddType("BIT", SqlType.Bit, 1, null, null, true);
				AddType("BOOLEAN", SqlType.Bit, 1, null, null, true);
				AddType("TINYINT", SqlType.TinyInt, 9, null, null, true);
				AddType("SMALLINT", SqlType.SmallInt, 9, null, null, true);
				AddType("INTEGER", SqlType.Integer, 9, null, null, true);
				AddType("BIGINT", SqlType.BigInt, 9, null, null, true);
				AddType("FLOAT", SqlType.Float, 9, null, null, true);
				AddType("REAL", SqlType.Real, 9, null, null, true);
				AddType("DOUBLE", SqlType.Double, 9, null, null, true);
				AddType("NUMERIC", SqlType.Numeric, 9, null, null, true);
				AddType("DECIMAL", SqlType.Decimal, 9, null, null, true);
				AddType("IDENTITY", SqlType.Identity, 9, null, null, true);
				AddType("CHAR", SqlType.Char, 9, "'", "'", true);
				AddType("VARCHAR", SqlType.VarChar, 9, "'", "'", true);
				AddType("LONGVARCHAR", SqlType.LongVarChar, 9, "'", "'", true);
				AddType("DATE", SqlType.Date, 9, null, null, true);
				AddType("TIME", SqlType.Time, 9, null, null, true);
				AddType("TIMESTAMP", SqlType.TimeStamp, 9, null, null, true);
				AddType("BINARY", SqlType.Binary, 9, null, null, false);
				AddType("VARBINARY", SqlType.VarBinary, 9, null, null, false);
				AddType("LONGVARBINARY", SqlType.LongVarBinary, 9, null, null, false);
				AddType("OBJECT", SqlType.Object, 9, null, null, false);
			}

			// ---------- Implemented from GTDataSource ----------

			public override DataTableInfo TableInfo {
				get { return SqlTypesTableInfo; }
			}

			public override int RowCount {
				get { return sqlTypes.Count / 6; }
			}

			public override TObject GetCell(int column, int row) {
				int i = (row * 6);
				SqlTypeInfo typeInfo = sqlTypes[row];

				switch (column) {
					case 0:  // type_name
						return GetColumnValue(column, typeInfo.Name);
					case 1:  // data_type
						return GetColumnValue(column, (int)typeInfo.Type);
					case 2:  // precision
						return GetColumnValue(column, typeInfo.Precision);
					case 3:  // literal_prefix
						return GetColumnValue(column, typeInfo.LiteralPrefix);
					case 4:  // literal_suffix
						return GetColumnValue(column, typeInfo.LiteralSuffix);
					case 5:  // create_params
						return GetColumnValue(column, null);
					case 6:  // nullable
						return GetColumnValue(column, TypeNullable);
					case 7:  // case_sensitive
						return GetColumnValue(column, true);
					case 8:  // searchable
						return GetColumnValue(column, typeInfo.Searchable);
					case 9:  // unsigned_attribute
						return GetColumnValue(column, false);
					case 10:  // fixed_prec_scale
						return GetColumnValue(column, false);
					case 11:  // auto_increment
						return GetColumnValue(column, typeInfo.Type == SqlType.Identity);
					case 12:  // local_type_name
						return GetColumnValue(column, null);
					case 13:  // minimum_scale
						return GetColumnValue(column, (BigNumber)0);
					case 14:  // maximum_scale
						return GetColumnValue(column, (BigNumber)10000000);
					case 15:  // sql_data_type
						return GetColumnValue(column, null);
					case 16:  // sql_datetype_sub
						return GetColumnValue(column, null);
					case 17:  // num_prec_radix
						return GetColumnValue(column, (BigNumber)10);
					default:
						throw new ApplicationException("Column out of bounds.");
				}
			}

			// ---------- Overwritten from GTDataSource ----------

			protected override void Dispose(bool disposing) {
				if (disposing) {
					sqlTypes = null;
					database = null;
				}
			}

			#region SqlTypeInfo

			class SqlTypeInfo {
				public string Name;
				public SqlType Type;
				public byte Precision;
				public string LiteralPrefix;
				public string LiteralSuffix;
				public byte Searchable;

			}

			#endregion
		}

		#endregion
	}
}
