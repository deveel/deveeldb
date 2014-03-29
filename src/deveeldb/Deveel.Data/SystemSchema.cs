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
			password.AddColumn("UserName", TType.StringType);
			password.AddColumn("Password", TType.StringType);

			DataTableInfo userPriv = new DataTableInfo(UserPrivileges);
			userPriv.AddColumn("UserName", TType.StringType);
			userPriv.AddColumn("PrivGroupName", TType.StringType);

			DataTableInfo userConnectPriv = new DataTableInfo(UserConnectPrivileges);
			userConnectPriv.AddColumn("UserName", TType.StringType);
			userConnectPriv.AddColumn("Protocol", TType.StringType);
			userConnectPriv.AddColumn("Host", TType.StringType);
			userConnectPriv.AddColumn("Access", TType.StringType);

			DataTableInfo grant = new DataTableInfo(Grant);
			grant.AddColumn("priv_bit", TType.NumericType);
			grant.AddColumn("object", TType.NumericType);
			grant.AddColumn("param", TType.StringType);
			grant.AddColumn("grantee", TType.StringType);
			grant.AddColumn("grant_option", TType.StringType);
			grant.AddColumn("granter", TType.StringType);

			DataTableInfo service = new DataTableInfo(Service);
			service.AddColumn("name", TType.StringType);
			service.AddColumn("class", TType.StringType);
			service.AddColumn("type", TType.StringType);

			DataTableInfo functionFactory = new DataTableInfo(Functionfactory);
			functionFactory.AddColumn("name", TType.StringType);
			functionFactory.AddColumn("class", TType.StringType);
			functionFactory.AddColumn("type", TType.StringType);

			DataTableInfo function = new DataTableInfo(Function);
			function.AddColumn("schema", TType.StringType);
			function.AddColumn("name", TType.StringType);
			function.AddColumn("type", TType.StringType);
			function.AddColumn("location", TType.StringType);
			function.AddColumn("return_type", TType.StringType);
			function.AddColumn("args_type", TType.StringType);
			function.AddColumn("username", TType.StringType);

			DataTableInfo view = new DataTableInfo(View);
			view.AddColumn("schema", TType.StringType);
			view.AddColumn("name", TType.StringType);
			view.AddColumn("query", TType.BinaryType);
			view.AddColumn("data", TType.BinaryType);
			view.AddColumn("username", TType.StringType);

			DataTableInfo label = new DataTableInfo(Label);
			label.AddColumn("object_type", TType.NumericType);
			label.AddColumn("object_name", TType.StringType);
			label.AddColumn("label", TType.StringType);

			DataTableInfo dataTrigger = new DataTableInfo(DataTrigger);
			dataTrigger.AddColumn("schema", TType.StringType);
			dataTrigger.AddColumn("name", TType.StringType);
			dataTrigger.AddColumn("type", TType.NumericType);
			dataTrigger.AddColumn("on_object", TType.StringType);
			dataTrigger.AddColumn("action", TType.StringType);
			dataTrigger.AddColumn("misc", TType.BinaryType);
			dataTrigger.AddColumn("username", TType.StringType);

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
			manager.Grant(global::Deveel.Data.Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.connection_info", GrantManager.PublicUsernameStr,
						  false, granter);
			manager.Grant(global::Deveel.Data.Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.current_connections",
						  GrantManager.PublicUsernameStr, false, granter);
			manager.Grant(global::Deveel.Data.Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.variables", GrantManager.PublicUsernameStr, false,
						  granter);
			manager.Grant(global::Deveel.Data.Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.database_stats",
						  GrantManager.PublicUsernameStr, false, granter);
			manager.Grant(global::Deveel.Data.Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.database_vars", GrantManager.PublicUsernameStr,
						  false, granter);
			manager.Grant(global::Deveel.Data.Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.product_info", GrantManager.PublicUsernameStr,
						  false, granter);
			manager.Grant(global::Deveel.Data.Security.Privileges.TableRead, GrantObject.Table, "SYSTEM.sql_types", GrantManager.PublicUsernameStr,
						  false, granter);
		}
	}
}
