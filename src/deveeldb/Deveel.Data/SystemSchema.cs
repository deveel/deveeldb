using System;

using Deveel.Data.DbSystem;
using Deveel.Data.Security;
using Deveel.Data.Types;

namespace Deveel.Data {
	public static class SystemSchema {
		/// <summary>
		/// The name of the system schema that contains tables refering to 
		/// system information.
		/// </summary>
		public const string Name = TableDataConglomerate.SystemSchema;

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

		internal static void CreateTables(DatabaseConnection connection) {
			// --- The user management tables ---
			DataTableInfo password = new DataTableInfo(Password);
			password.AddColumn("UserName", PrimitiveTypes.VarString);
			password.AddColumn("Password", PrimitiveTypes.VarString);
			password.AddColumn("Salt", PrimitiveTypes.VarString);
			password.AddColumn("Hash", PrimitiveTypes.VarString);

			DataTableInfo userPriv = new DataTableInfo(UserPrivileges);
			userPriv.AddColumn("UserName", PrimitiveTypes.VarString);
			userPriv.AddColumn("PrivGroupName", PrimitiveTypes.VarString);

			DataTableInfo userConnectPriv = new DataTableInfo(UserConnectPrivileges);
			userConnectPriv.AddColumn("UserName", PrimitiveTypes.VarString);
			userConnectPriv.AddColumn("Protocol", PrimitiveTypes.VarString);
			userConnectPriv.AddColumn("Host", PrimitiveTypes.VarString);
			userConnectPriv.AddColumn("Access", PrimitiveTypes.VarString);

			DataTableInfo grant = new DataTableInfo(Grant);
			grant.AddColumn("priv_bit", PrimitiveTypes.Numeric);
			grant.AddColumn("object", PrimitiveTypes.Numeric);
			grant.AddColumn("param", PrimitiveTypes.VarString);
			grant.AddColumn("grantee", PrimitiveTypes.VarString);
			grant.AddColumn("grant_option", PrimitiveTypes.VarString);
			grant.AddColumn("granter", PrimitiveTypes.VarString);

			DataTableInfo service = new DataTableInfo(Service);
			service.AddColumn("name", PrimitiveTypes.VarString);
			service.AddColumn("class", PrimitiveTypes.VarString);
			service.AddColumn("type", PrimitiveTypes.VarString);

			DataTableInfo functionFactory = new DataTableInfo(Functionfactory);
			functionFactory.AddColumn("name", PrimitiveTypes.VarString);
			functionFactory.AddColumn("class", PrimitiveTypes.VarString);
			functionFactory.AddColumn("type", PrimitiveTypes.VarString);

			DataTableInfo function = new DataTableInfo(Function);
			function.AddColumn("schema", PrimitiveTypes.VarString);
			function.AddColumn("name", PrimitiveTypes.VarString);
			function.AddColumn("type", PrimitiveTypes.VarString);
			function.AddColumn("location", PrimitiveTypes.VarString);
			function.AddColumn("return_type", PrimitiveTypes.VarString);
			function.AddColumn("args_type", PrimitiveTypes.VarString);
			function.AddColumn("username", PrimitiveTypes.VarString);

			DataTableInfo view = new DataTableInfo(View);
			view.AddColumn("schema", PrimitiveTypes.VarString);
			view.AddColumn("name", PrimitiveTypes.VarString);
			view.AddColumn("query", PrimitiveTypes.BinaryType);
			view.AddColumn("data", PrimitiveTypes.BinaryType);
			view.AddColumn("username", PrimitiveTypes.VarString);

			DataTableInfo label = new DataTableInfo(Label);
			label.AddColumn("object_type", PrimitiveTypes.Numeric);
			label.AddColumn("object_name", PrimitiveTypes.VarString);
			label.AddColumn("label", PrimitiveTypes.VarString);

			DataTableInfo dataTrigger = new DataTableInfo(DataTrigger);
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
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.connection_info", GrantManager.PublicUsernameStr,
						  false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.current_connections",
						  GrantManager.PublicUsernameStr, false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.variables", GrantManager.PublicUsernameStr, false,
						  granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.database_stats",
						  GrantManager.PublicUsernameStr, false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.database_vars", GrantManager.PublicUsernameStr,
						  false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.product_info", GrantManager.PublicUsernameStr,
						  false, granter);
			manager.Grant(Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.sql_types", GrantManager.PublicUsernameStr,
						  false, granter);
		}
	}
}
