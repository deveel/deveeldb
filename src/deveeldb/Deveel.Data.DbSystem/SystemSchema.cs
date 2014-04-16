using System;

using Deveel.Data.Procedures;
using Deveel.Data.Routines;
using Deveel.Data.Security;
using Deveel.Data.Transactions;
using Deveel.Data.Types;

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

		internal static void AddSystemTables(Transaction transaction) {
			DataTableInfo tableInfo;

			// SYSTEM.SEQUENCE_INFO
			tableInfo = new DataTableInfo(SysSequenceInfo);
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

		internal static void Initialize(Transaction transaction) {
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
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.connection_info", User.PublicName,
						  false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.current_connections",
						  User.PublicName, false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.variables", User.PublicName, false,
						  granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.database_stats",
						  User.PublicName, false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.database_vars", User.PublicName,
						  false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.product_info", User.PublicName,
						  false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.sql_types", User.PublicName,
						  false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.function", User.PublicName, false, granter);
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
				Routines.Function.ExternalName(typeof(SystemBackup)),
				PrimitiveTypes.VarString,
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
	}
}
