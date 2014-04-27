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

using Deveel.Data.Query;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Threading;
using Deveel.Data.Transactions;
using Deveel.Data.Types;
using Deveel.Diagnostics;

namespace Deveel.Data.DbSystem {
	public static class DatabaseConnectionExtensions {
		internal static ICommitableTransaction CommittableTransaction(this IDatabaseConnection connection) {
			var committable = connection.Transaction as ICommitableTransaction;
			if (committable == null)
				throw new InvalidOperationException("A transaction was not open or it's not commitable.");

			return committable;
		}

		public static void AssertExclusive(this IDatabaseConnection connection) {
			if (!connection.LockingMechanism.IsInExclusiveMode)
				throw new SecurityException("Assertion failed: Expected to be in exclusive mode.");
		}

		/// <summary>
		/// Generates an exception if the name of the table is reserved and the
		/// creation of the table should be prevented.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="tableName"></param>
		/// <remarks>
		/// For example, the table names <c>OLD</c> and <c>NEW</c> are reserved.
		/// </remarks>
		public static void AssertAllowCreate(this IDatabaseConnection connection, TableName tableName) {
			// We do not allow tables to be created with a reserved name
			String name = tableName.Name;
			if (String.Compare(name, "OLD", StringComparison.OrdinalIgnoreCase) == 0 ||
				String.Compare(name, "NEW", StringComparison.OrdinalIgnoreCase) == 0) {
				throw new SecurityException("Table name '" + tableName + "' is reserved.");
			}
		}

		public static void OnDatabaseObjectCreated(this IDatabaseConnection connection, TableName objectName) {
			connection.CommittableTransaction().OnDatabaseObjectCreated(objectName);
		}

		public static void OnDatabaseObjectDropped(this IDatabaseConnection connection, TableName objectName) {
			connection.CommittableTransaction().OnDatabaseObjectDropped(objectName);
		}

		#region Schemata

		/// <summary>
		/// Changes the default schema to the given schema.
		/// </summary>
		/// <param name="schemaName"></param>
		public static void SetDefaultSchema(this IDatabaseConnection connection, string schemaName) {
			bool ignoreCase = connection.IsInCaseInsensitiveMode;
			SchemaDef schema = connection.ResolveSchemaCase(schemaName, ignoreCase);
			if (schema == null)
				throw new ApplicationException("Schema '" + schemaName + "' does not exist.");

			// Set the default schema for this connection
			connection.CurrentSchema = schema.Name;
		}

		public static void CreateSchema(this IDatabaseConnection connection, string name, String type) {
			// Assert
		 	connection.AssertExclusive();
			connection.CommittableTransaction().CreateSchema(name, type);
		}

		public static void DropSchema(this IDatabaseConnection connection, string name) {
			// Assert
			connection.AssertExclusive();
			connection.CommittableTransaction().DropSchema(name);
		}

		public static bool SchemaExists(this IDatabaseConnection connection, string name) {
			return connection.Transaction.SchemaExists(name);
		}

		public static SchemaDef ResolveSchemaCase(this IDatabaseConnection connection, string name, bool ignoreCase) {
			return connection.Transaction.ResolveSchemaCase(name, ignoreCase);
		}

		public static SchemaDef ResolveSchemaNames(this IDatabaseConnection connection, string name) {
			bool ignoreCase = connection.IsInCaseInsensitiveMode;
			return connection.ResolveSchemaCase(name, ignoreCase);
		}

		public static SchemaDef[] GetSchemaList(this IDatabaseConnection connection) {
			return connection.Transaction.GetSchemaList();
		}


		#endregion

		#region Tables

		/// <summary>
		/// If the given table name is a reserved name, then we must substitute it
		/// with its correct form.
		/// </summary>
		/// <param name="tableName"></param>
		/// <returns></returns>
		public static TableName SubstituteReservedTableName(this IDatabaseConnection connection, TableName tableName) {
			// We do not allow tables to be created with a reserved name
			String name = tableName.Name;
			if (String.Compare(name, "OLD", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.OldTriggerTable;
			if (String.Compare(name, "NEW", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.NewTriggerTable;
			return tableName;
		}

		/// <summary>
		/// Gets an array of <see cref="TableName"/> that contains the 
		/// list of database tables visible by the underlying transaction.
		/// </summary>
		/// <remarks>
		/// The list returned represents all the queriable tables in
		/// the database.
		/// </remarks>
		public static TableName[] GetTables(this IDatabaseConnection connection) {
			return connection.Transaction.GetTables();
		}

		/// <summary>
		/// Checks the existence of a table within the underlying transaction.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="tableName">Name of the table to check.</param>
		/// <remarks>
		/// This method checks if the table exists within the <see cref="IDatabaseConnection.CurrentSchema"/>
		/// of the session.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if a table with the given <paramref name="tableName"/>
		/// exists within the underlying transaction, otherwise <b>false</b>.
		/// </returns>
		public static bool TableExists(this IDatabaseConnection connection, string tableName) {
			return connection.TableExists(new TableName(connection.CurrentSchema, tableName));
		}

		/// <summary>
		/// Checks the existence of a table within the underlying transaction.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="tableName">Name of the table to check.</param>
		/// <returns>
		/// Returns <b>true</b> if a table with the given <paramref name="tableName"/>
		/// exists within the underlying transaction, otherwise <b>false</b>.
		/// </returns>
		public static bool TableExists(this IDatabaseConnection connection, TableName tableName) {
			tableName = connection.SubstituteReservedTableName(tableName);
			return connection.Transaction.TableExists(tableName);
		}

		/// <summary>
		/// Gets the type of he given table.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="tableName">Name of the table to get the type.</param>
		/// <remarks>
		/// Currently this is either <i>TABLE</i> or <i>VIEW</i>.
		/// </remarks>
		/// <returns>
		/// Returns a string describing the type of the table identified by the
		/// given <paramref name="tableName"/>.
		/// </returns>
		/// <exception cref="StatementException">
		/// If none table with the given <paramref name="tableName"/> was found 
		/// in the underlying transaction.</exception>
		public static string GetTableType(this IDatabaseConnection connection, TableName tableName) {
			tableName = connection.SubstituteReservedTableName(tableName);
			return connection.Transaction.GetTableType(tableName);
		}

		/// <summary>
		/// Attempts to resolve the given table name to its correct case assuming
		/// the table name represents a case insensitive version of the name.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="tableName">Table name to resolve.</param>
		/// <remarks>
		/// For example, <c>aPP.CuSTOMer</c> may resolve to <c>default.Customer</c>.
		/// If the table name can not resolve to a valid identifier it returns 
		/// the input table name
		/// The actual presence of the table should always be checked by 
		/// calling <see cref="TableExists(TableName)"/> after the  method 
		/// returns.
		/// </remarks>
		/// <returns>
		/// Returns a properly formatted <see cref="TableName"/> if was able to
		/// resolve the given <paramref name="tableName"/>, otherwise returns
		/// the input table name.
		/// </returns>
		public static TableName TryResolveCase(this IDatabaseConnection connection, TableName tableName) {
			tableName = connection.SubstituteReservedTableName(tableName);
			tableName = connection.Transaction.TryResolveCase(tableName);
			return tableName;
		}

		/// <summary>
		/// Resolves a table name.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="name">Name of the table to resolve.</param>
		/// <remarks>
		/// If the schema part of the table name is not present then it is set 
		/// to the <see cref="CurrentSchema"/> of the database session.
		/// If the database is ignoring the case then this will correctly resolve 
		/// the table to the cased version of the table name.
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="TableName"/> representing the properly
		/// formatted table name.
		/// </returns>
		public static TableName ResolveTableName(this IDatabaseConnection connection, string name) {
			TableName tableName = TableName.Resolve(connection.CurrentSchema, name);
			tableName = connection.SubstituteReservedTableName(tableName);
			if (connection.IsInCaseInsensitiveMode) {
				// Try and resolve the case of the table name,
				tableName = connection.TryResolveCase(tableName);
			}
			return tableName;
		}

		/// <summary>
		/// Resolves the given string to a table name
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="name">Table name to resolve.</param>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If the reference is ambigous or
		/// </exception>
		public static TableName ResolveToTableName(this IDatabaseConnection connection, string name) {
			TableName tableName = TableName.Resolve(connection.CurrentSchema, name);
			if (String.Compare(tableName.Name, "OLD", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.OldTriggerTable;
			if (String.Compare(tableName.Name, "NEW", StringComparison.OrdinalIgnoreCase) == 0)
				return SystemSchema.NewTriggerTable;

			return connection.Transaction.ResolveToTableName(connection.CurrentSchema, name, connection.IsInCaseInsensitiveMode);

		}

		/// <summary>
		/// Gets the meta informations for the given table.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="name">Name of the table to return the 
		/// meta informations.</param>
		/// <returns>
		/// Returns the <see cref="DataTableInfo"/> representing the meta 
		/// informations for the tabl identified by <paramref name="name"/> 
		/// if found, otherwise <b>null</b>.
		/// </returns>
		public static DataTableInfo GetTableInfo(this IDatabaseConnection connection, TableName name) {
			name = connection.SubstituteReservedTableName(name);
			return connection.Transaction.GetTableInfo(name);
		}

		/// <summary>
		/// Gets the table for the given name.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="tableName">Name of the table to return.</param>
		/// <remarks>
		/// This method uses the <see cref="IDatabaseConnection.CurrentSchema"/> to get the table.
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="DataTable"/> that represents the table 
		/// identified by <paramref name="tableName"/>, otherwise returns <b>null</b>.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If none table was found for the given <paramref name="tableName"/>.
		/// </exception>
		public static DataTable GetTable(this IDatabaseConnection connection, string tableName) {
			return connection.GetTable(new TableName(connection.CurrentSchema, tableName));
		}

		/// <summary>
		/// Gets the table for the given name.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="name">Name of the table to return.</param>
		/// <returns>
		/// Returns a <see cref="DataTable"/> that represents the table 
		/// identified by <paramref name="name"/>, otherwise returns 
		/// <b>null</b>.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If none table was found for the given <paramref name="name"/>.
		/// </exception>
		public static DataTable GetTable(this IDatabaseConnection connection, TableName name) {
			name = connection.SubstituteReservedTableName(name);

			try {
				// Special handling of NEW and OLD table, we cache the DataTable in the
				// OldNewTableState object,
				if (name.Equals(SystemSchema.OldTriggerTable))
					return connection.OldNewState.OldDataTable ??
					       (connection.OldNewState.OldDataTable = new DataTable(connection, connection.Transaction.GetTable(name)));
				if (name.Equals(SystemSchema.NewTriggerTable))
					return connection.OldNewState.NewDataTable ??
					       (connection.OldNewState.NewDataTable = new DataTable(connection, connection.Transaction.GetTable(name)));

				// Ask the transaction for the table
				ITableDataSource table = connection.Transaction.GetTable(name);

				// if not found in the transaction return null
				if (table == null)
					return null;

				// Is this table in the tables_cache?
				ITableDataSource dtable = connection.GetCachedTable(name);
				if (dtable == null) {
					// No, so wrap it around a Datatable and WriteByte it in the cache
					dtable = new DataTable(connection, table);
					connection.CacheTable(name, dtable);
				}

				// Return the DataTable
				return (DataTable) dtable;

			} catch (DatabaseException e) {
				connection.Transaction.Context.SystemContext.Logger.Error(connection, e);
				throw new ApplicationException("Database Exception: " + e.Message, e);
			}

		}


		/// <summary>
		/// Creates a new temporary table within the context of the transaction.
		/// </summary>
		/// <param name="tableInfo">Table meta informations for creating the table.</param>
		/// <remarks>
		/// A temporary table is a fully functional table, which persists for all the lifetime
		/// of a transaction and that is disposed (both structure and data) at the end of the
		/// parent transaction.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the name of the table is reserved and the creation of the table 
		/// should be prevented.
		/// </exception>
		public static void CreateTemporaryTable(this IDatabaseConnection connection, DataTableInfo tableInfo) {
			connection.AssertAllowCreate(tableInfo.TableName);
			connection.CommittableTransaction().CreateTemporaryTable(tableInfo);
		}

		/// <summary>
		/// Creates a new table within the context of the transaction.
		/// </summary>
		/// <param name="tableInfo">Table meta informations for creating the table.</param>
		/// <exception cref="StatementException">
		/// If the name of the table is reserved and the creation of the table 
		/// should be prevented.
		/// </exception>
		public static void CreateTable(this IDatabaseConnection connection, DataTableInfo tableInfo) {
			connection.AssertAllowCreate(tableInfo.TableName);
			connection.CommittableTransaction().CreateTable(tableInfo);
		}

		/// <summary>
		/// Creates a new table within this transaction with the given 
		/// sector size.
		/// </summary>
		/// <param name="tableInfo">Meta informations used to create the table.</param>
		/// <param name="dataSectorSize">Size of data sectors of the table.</param>
		/// <param name="indexSectorSize">Size of the index sectors of the table.</param>
		/// <remarks>
		/// This should only be used as very fine grain optimization for 
		/// creating tables. 
		/// If in the future the underlying table model is changed so that the given
		/// <paramref name="dataSectorSize"/> value is unapplicable, then the value 
		/// will be ignored.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If a table with the same name (specified by <paramref name="tableInfo"/>) 
		/// already exists.
		/// </exception>
		public static void CreateTable(this IDatabaseConnection connection, DataTableInfo tableInfo, int dataSectorSize, int indexSectorSize) {
			connection.AssertAllowCreate(tableInfo.TableName);
			connection.CommittableTransaction().CreateTable(tableInfo, dataSectorSize, indexSectorSize);
		}

		/// <summary>
		/// Alters a table within the underlying transaction.
		/// </summary>
		/// <param name="tableInfo">Table metadata informations for aletring the table</param>
		/// <exception cref="StatementException">
		/// If the name of the table is reserved and the creation of the table 
		/// should be prevented.
		/// </exception>
		public static void UpdateTable(this IDatabaseConnection connection, DataTableInfo tableInfo) {
			connection.AssertAllowCreate(tableInfo.TableName);
			connection.CommittableTransaction().AlterTable(tableInfo.TableName, tableInfo);
		}

		/// <summary>
		/// Alters a table within the underlying transaction.
		/// </summary>
		/// <param name="tableInfo">Table metadata informations for altering 
		/// the table.</param>
		/// <param name="dataSectorSize"></param>
		/// <param name="indexSectorSize"></param>
		/// <remarks>
		/// This should only be used as very fine grain optimization
		/// for creating tables. If in the future the underlying table model is
		/// changed so that the given <paramref name="dataSectorSize"/> value 
		/// is unapplicable, then the value will be ignored.
		/// </remarks>
		public static void UpdateTable(this IDatabaseConnection connection, DataTableInfo tableInfo, int dataSectorSize, int indexSectorSize) {
			connection.AssertAllowCreate(tableInfo.TableName);
			connection.CommittableTransaction().AlterTable(tableInfo.TableName, tableInfo, dataSectorSize, indexSectorSize);
		}

		/// <summary>
		/// If a table exists with the given table name (defined by <paramref name="tableInfo"/>)
		/// alters its the structure, otherwise creates a new table.
		/// </summary>
		/// <param name="tableInfo">Meta informations for altering or creating a table.</param>
		/// <param name="dataSectorSize">Size of data sectors of the table.</param>
		/// <param name="indexSectorSize">Size of the index sectors of the table.</param>
		/// <remarks>
		/// This should only be used as very fine grain optimization for creating or
		/// altering tables.
		/// If in the future the underlying table model is changed so that the given 
		/// <paramref name="dataSectorSize"/> and <paramref name="indexSectorSize"/> 
		/// values are unapplicable and will be ignored.
		/// </remarks>
		public static void AlterCreateTable(this IDatabaseConnection connection, DataTableInfo tableInfo, int dataSectorSize, int indexSectorSize) {
			if (!connection.TableExists(tableInfo.TableName)) {
				connection.CreateTable(tableInfo, dataSectorSize, indexSectorSize);
			} else {
				connection.UpdateTable(tableInfo, dataSectorSize, indexSectorSize);
			}
		}

		/// <summary>
		/// If a table exists with the given table name (defined by <paramref name="tableInfo"/>)
		/// alters its the structure, otherwise creates a new table.
		/// </summary>
		/// <param name="tableInfo">Meta informations for altering or creating a table.</param>
		/// <exception cref="StatementException"></exception>
		public static void AlterCreateTable(this IDatabaseConnection connection, DataTableInfo tableInfo) {
			if (!connection.TableExists(tableInfo.TableName)) {
				connection.CreateTable(tableInfo);
			} else {
				connection.UpdateTable(tableInfo);
			}
		}

		/// <summary>
		/// Drops a table within the transaction.
		/// </summary>
		/// <param name="tableName">Name of the table to drop.</param>
		/// <exception cref="TransactionException">
		/// If none tables was found for the given <paramref name="tableName"/>.
		/// </exception>
		public static void DropTable(this IDatabaseConnection connection, string tableName) {
			connection.DropTable(new TableName(connection.CurrentSchema, tableName));
		}

		/// <summary>
		/// Drops a table within the transaction.
		/// </summary>
		/// <param name="tableName">Name of the table to drop.</param>
		/// <exception cref="TransactionException">
		/// If none tables was found for the given <paramref name="tableName"/>.
		/// </exception>
		public static void DropTable(this IDatabaseConnection connection, TableName tableName) {
			connection.CommittableTransaction().DropTable(tableName);
		}

		/// <summary>
		/// Compacts a table with the given name.
		/// </summary>
		/// <param name="tableName">The name of the table to compact.</param>
		/// <exception cref="StatementException">
		/// If none table was found for the given <paramref name="tableName"/> 
		/// in the <see cref="CurrentSchema"/>.
		/// </exception>
		public static void CompactTable(this IDatabaseConnection connection, string tableName) {
			connection.CompactTable(new TableName(connection.CurrentSchema, tableName));
		}

		/// <summary>
		/// Compacts a table with the given name.
		/// </summary>
		/// <param name="tableName">The name of the table to compact.</param>
		/// <exception cref="StatementException">
		/// If none table was found for the given <paramref name="tableName"/>.
		/// </exception>
		public static void CompactTable(this IDatabaseConnection connection, TableName tableName) {
			connection.CommittableTransaction().CompactTable(tableName);
		}

		///<summary>
		/// Adds the given table name to the list of tables that are selected from
		/// within the transaction in this connection.
		///</summary>
		///<param name="tableName"></param>
		public static void AddSelectedFromTable(this IDatabaseConnection connection, string tableName) {
			connection.AddSelectedFromTable(new TableName(connection.CurrentSchema, tableName));
		}

		///<summary>
		/// Adds the given table name to the list of tables that are selected from
		/// within the transaction in this connection.
		///</summary>
		///<param name="name"></param>
		public static void AddSelectedFromTable(this IDatabaseConnection connection, TableName name) {
			connection.CommittableTransaction().AddSelectedFromTable(name);
		}

		/// <summary>
		/// Returns a <see cref="ITableQueryInfo"/> that describes the 
		/// characteristics of a table including the name, the columns and the 
		/// query plan to produce the table.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="aliasedAs">Used to overwrite the default name of 
		/// the table object.</param>
		/// <remarks>
		/// This object can be used to resolve information about a 
		/// particular table, and to evaluate the query plan to produce 
		/// the table itself.
		/// <para>
		/// This produces <see cref="ITableQueryInfo"/> objects for all table 
		/// objects in the database including data tables and views.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public static ITableQueryInfo GetTableQueryInfo(this IDatabaseConnection connection, TableName tableName, TableName aliasedAs) {
			// Produce the data table info for this database object.
			DataTableInfo tableInfo = connection.GetTableInfo(tableName);
			// If the table is aliased, set a new DataTableInfo with the given name
			if (aliasedAs != null) {
				tableInfo = tableInfo.Clone(aliasedAs);
				tableInfo.IsReadOnly = true;
			}

			return new TableQueryInfo(connection, tableInfo, tableName, aliasedAs);
		}

		/// <summary>
		/// Creates a <see cref="IQueryPlanNode"/> to fetch the given table 
		/// object from the session.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="aliasedName"></param>
		/// <returns></returns>
		public static IQueryPlanNode CreateObjectFetchQueryPlan(this IDatabaseConnection connection, TableName tableName, TableName aliasedName) {
			string tableType = connection.GetTableType(tableName);
			if (tableType.Equals("VIEW"))
				return new FetchViewNode(tableName, aliasedName);

			return new FetchTableNode(tableName, aliasedName);
		}


		private class TableQueryInfo : ITableQueryInfo {
			private readonly IDatabaseConnection conn;
			private readonly DataTableInfo tableInfo;
			private readonly TableName tableName;
			private readonly TableName aliasedAs;

			public TableQueryInfo(IDatabaseConnection conn, DataTableInfo tableInfo, TableName tableName, TableName aliasedAs) {
				this.conn = conn;
				this.tableInfo = tableInfo;
				this.aliasedAs = aliasedAs;
				this.tableName = tableName;
			}

			public DataTableInfo TableInfo {
				get { return tableInfo; }
			}

			public IQueryPlanNode QueryPlanNode {
				get { return conn.CreateObjectFetchQueryPlan(tableName, aliasedAs); }
			}
		}

		#endregion

		#region Views

		/// <summary>
		/// Creates a new view.
		/// </summary>
		/// <param name="connection"></param>
		/// <param name="query"></param>
		/// <param name="view">View meta informations used to create the view.</param>
		/// <remarks>
		/// Note that this is a transactional operation. You need to commit for 
		/// the view to be visible to other transactions.
		/// </remarks>
		/// <exception cref="DatabaseException"/>
		public static void CreateView(this IDatabaseConnection connection, SqlQuery query, View view) {
			connection.AssertAllowCreate(view.TableInfo.TableName);

			try {
				connection.ViewManager.DefineView(view, query, connection.User);
			} catch (DatabaseException e) {
				connection.Database.Context.Logger.Error(connection, e);
				throw new Exception("Database Exception: " + e.Message, e);
			}

		}

		/// <summary>
		/// Drops a view with the given name.
		/// </summary>
		/// <param name="viewName">Name of the view to drop.</param>
		/// <remarks>
		/// Note that this is a transactional operation. You need to commit 
		/// for the change to be visible to other transactions.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if the drop succeeded, otherwise <b>false</b> if 
		/// the view was not found.
		/// </returns>
		public static bool DropView(this IDatabaseConnection connection, TableName viewName) {
			try {
				return connection.ViewManager.DeleteView(viewName);
			} catch (DatabaseException e) {
				connection.Database.Context.Logger.Error(connection, e);
				throw new Exception("Database Exception: " + e.Message, e);
			}

		}

		/// <summary>
		/// Returns a freshly deserialized IQueryPlanNode object for the given view
		/// object.
		/// </summary>
		/// <param name="tableName">Name of the view to return the query plan node.</param>
		/// <returns></returns>
		internal static IQueryPlanNode CreateViewQueryPlanNode(this IDatabaseConnection connection, TableName tableName) {
			return connection.ViewManager.CreateViewQueryPlanNode(tableName);
		}

		#endregion

		#region Constraints

		/// <summary>
		/// Checks all the rows in the table for immediate constraint violations
		/// and when the transaction is next committed check for all deferred
		/// constraint violations.
		/// </summary>
		/// <param name="tableName">Name of the table to check the constraints.</param>
		/// <remarks>
		/// This method is used when the constraints on a table changes and we 
		/// need to determine if any constraint violations occurred.
		/// <para>
		/// To the constraint checking system, this is like adding all the 
		/// rows to the given table.
		/// </para>
		/// </remarks>
		/// <exception cref="StatementException">
		/// If none table with the given <paramref name="tableName"/> was found.
		/// </exception>
		public static void CheckAllConstraints(this IDatabaseConnection connection, TableName tableName) {
			// Assert
			connection.AssertExclusive();
			connection.CommittableTransaction().CheckAllConstraints(tableName);
		}

		public static void AddUniqueConstraint(this IDatabaseConnection connection, TableName tableName, string[] columns, ConstraintDeferrability deferred, string constraintName) {
			// Assert
			connection.AssertExclusive();
			connection.CommittableTransaction().AddUniqueConstraint(tableName, columns, deferred, constraintName);
		}

		public static void AddForeignKeyConstraint(this IDatabaseConnection connection, TableName table, string[] columns,
			TableName refTable, string[] refColumns,
			ConstraintAction deleteRule, ConstraintAction updateRule,
			ConstraintDeferrability deferred, string constraintName) {
			// Assert
			connection.AssertExclusive();
			connection.CommittableTransaction().AddForeignKeyConstraint(table, columns, refTable, refColumns, deleteRule, updateRule, deferred, constraintName);
		}

		public static void AddPrimaryKeyConstraint(this IDatabaseConnection connection, TableName tableName, string[] columns, ConstraintDeferrability deferred, string constraintName) {
			// Assert
			connection.AssertExclusive();
			connection.CommittableTransaction().AddPrimaryKeyConstraint(tableName, columns, deferred, constraintName);
		}

		public static void AddCheckConstraint(this IDatabaseConnection connection, TableName tableName, Expression expression, ConstraintDeferrability deferred, String constraintName) {
			// Assert
			connection.AssertExclusive();
			connection.CommittableTransaction().AddCheckConstraint(tableName, expression, deferred, constraintName);
		}

		public static void DropAllConstraintsForTable(this IDatabaseConnection connection, TableName tableName) {
			// Assert
			connection.AssertExclusive();
			connection.CommittableTransaction().DropAllConstraintsForTable(tableName);
		}

		public static int DropNamedConstraint(this IDatabaseConnection connection, TableName tableName, string constraintName) {
			// Assert
			connection.AssertExclusive();
			return connection.CommittableTransaction().DropNamedConstraint(tableName, constraintName);
		}

		public static bool DropPrimaryKeyConstraintForTable(this IDatabaseConnection connection, TableName tableName, string constraintName) {
			// Assert
			connection.AssertExclusive();
			return connection.CommittableTransaction().DropPrimaryKeyConstraintForTable(tableName, constraintName);
		}

		public static TableName[] QueryTablesRelationallyLinkedTo(this IDatabaseConnection connection, TableName table) {
			return connection.Transaction.QueryTablesRelationallyLinkedTo(table);
		}

		public static DataConstraintInfo[] QueryTableUniqueGroups(this IDatabaseConnection connection, TableName tableName) {
			return connection.Transaction.QueryTableUniques(tableName);
		}

		public static DataConstraintInfo QueryTablePrimaryKeyGroup(this IDatabaseConnection connection, TableName tableName) {
			return connection.Transaction.QueryTablePrimaryKey(tableName);
		}

		public static DataConstraintInfo[] QueryTableCheckExpressions(this IDatabaseConnection connection, TableName tableName) {
			return connection.Transaction.QueryTableCheckExpressions(tableName);
		}

		public static DataConstraintInfo[] QueryTableForeignKeyReferences(this IDatabaseConnection connection, TableName tableName) {
			return connection.Transaction.QueryTableForeignKeys(tableName);
		}

		public static DataConstraintInfo[] QueryTableImportedForeignKeyReferences(this IDatabaseConnection connection, TableName tableName) {
			return connection.Transaction.QueryTableImportedForeignKeys(tableName);
		}

		#endregion

		#region Sequences

		/// <summary>
		/// Requests the sequence generator for the next value.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </remarks>
		/// <returns></returns>
		public static long NextSequenceValue(this IDatabaseConnection connection, string name) {
			// Resolve and ambiguity test
			TableName seqName = connection.ResolveToTableName(name);
			return connection.Transaction.NextSequenceValue(seqName);
		}

		/// <summary>
		/// Returns the current sequence value for the given sequence generator.
		/// </summary>
		/// <param name="name"></param>
		/// <remarks>
		/// The value returned is the same value returned by <see cref="NextSequenceValue"/>.
		/// <para>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If no value was returned by <see cref="NextSequenceValue"/>.
		/// </exception>
		public static long LastSequenceValue(this IDatabaseConnection connection, string name) {
			// Resolve and ambiguity test
			TableName seqName = connection.ResolveToTableName(name);
			return connection.Transaction.LastSequenceValue(seqName);
		}

		/// <summary>
		/// Sets the sequence value for the given sequence generator.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="value"></param>
		/// <remarks>
		/// <b>Note:</b> This does <b>note</b> check that the user owning 
		/// the session has the correct privileges to perform the operation.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the generator does not exist or it is not possible to set the 
		/// value for the generator.
		/// </exception>
		public static void SetSequenceValue(this IDatabaseConnection connection, string name, long value) {
			// Resolve and ambiguity test
			TableName seqName = connection.ResolveToTableName(name);
			connection.Transaction.SetSequenceValue(seqName, value);
		}

		/// <summary>
		/// Returns the next unique identifier for the given table from 
		/// the schema.
		/// </summary>
		/// <param name="name"></param>
		/// <returns></returns>
		public static long NextUniqueId(this IDatabaseConnection connection, TableName name) {
			return connection.Transaction.NextUniqueId(name);
		}

		/// <summary>
		/// Returns the next unique identifier for the given table from 
		/// the current schema.
		/// </summary>
		/// <param name="tableName"></param>
		/// <returns></returns>
		public static long NextUniqueId(this IDatabaseConnection connection, string tableName) {
			TableName tname = TableName.Resolve(connection.CurrentSchema, tableName);
			return connection.NextUniqueId(tname);
		}

		/// <summary>
		/// Returns the current unique identifier for the given table from
		/// the current schema.
		/// </summary>
		/// <param name="tableName"></param>
		/// <returns></returns>
		public static long CurrentUniqueId(this IDatabaseConnection connection, TableName tableName) {
			return connection.Transaction.CurrentUniqueId(tableName);
		}

		public static long CurrentUniqueId(this IDatabaseConnection connection, string tableName) {
			return connection.CurrentUniqueId(TableName.Resolve(connection.CurrentSchema, tableName));
		}

		/// <summary>
		/// Creates a new sequence generator with the given name and initializes 
		/// it with the given details.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="startValue"></param>
		/// <param name="incrementBy"></param>
		/// <param name="minValue"></param>
		/// <param name="maxValue"></param>
		/// <param name="cache"></param>
		/// <param name="cycle"></param>
		/// <remarks>
		/// This does <b>not</b> check if the given name clashes with an 
		/// existing database object.
		/// </remarks>
		public static void CreateSequenceGenerator(this IDatabaseConnection connection, TableName name, long startValue, long incrementBy, long minValue, long maxValue, long cache, bool cycle) {
			// Check the name of the database object isn't reserved (OLD/NEW)
			connection.AssertAllowCreate(name);
			connection.CommittableTransaction().CreateSequenceGenerator(name, startValue, incrementBy, minValue, maxValue, cache, cycle);
		}

		/// <summary>
		/// Drops an existing sequence generator with the given name.
		/// </summary>
		/// <param name="name"></param>
		public static void DropSequenceGenerator(this IDatabaseConnection connection, TableName name) {
			connection.CommittableTransaction().DropSequenceGenerator(name);
		}

		#endregion

		#region Variables

		public static Variable DeclareVariable(this IDatabaseConnection connection, string name, TType type, bool constant, bool notNull) {
			return connection.Transaction.Variables.DeclareVariable(name, type, constant, notNull);
		}

		public static Variable DeclareVariable(this IDatabaseConnection connection, string name, TType type, bool notNull) {
			return connection.DeclareVariable(name, type, false, notNull);
		}

		public static Variable DeclareVariable(this IDatabaseConnection connection, string name, TType type) {
			return connection.DeclareVariable(name, type, false);
		}

		/// <summary>
		/// Evaluates the expression to a bool value (true or false).
		/// </summary>
		/// <param name="exp"></param>
		/// <param name="context"></param>
		/// <returns></returns>
		private static bool ToBooleanValue(Expression exp, IQueryContext context) {
			var value = exp.Evaluate(null, null, context);
			if (value.IsNull)
				throw new StatementException("Expression does not evaluate to a bool (true or false).");

			if (value.TType is TBooleanType)
				return value.ToBoolean();

			if (value.TType is TNumericType) {
				var iValue = value.ToBigNumber().ToInt32();
				if (iValue == 0)
					return false;
				if (iValue == 1)
					return true;
			}

			throw new StatementException("Expression does not evaluate to a bool (true or false).");
		}

		/// <summary>
		/// Assigns a variable to the expression for the session.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="exp"></param>
		/// <param name="context">A context used to evaluate the expression
		/// forming the value of the variable.</param>
		/// <remarks>
		/// This is a generic way of setting properties of the session.
		/// <para>
		/// Special variables, that are recalled by the system, are:
		/// <list type="bullet">
		/// <item><c>ERROR_ON_DIRTY_SELECT</c>: set to <b>true</b> for turning 
		/// the transaction conflict off on the session.</item>
		/// <item><c>CASE_INSENSITIVE_IDENTIFIERS</c>: <b>true</b> means the grammar 
		/// becomes case insensitive for identifiers resolved by the 
		/// grammar.</item>
		/// </list>
		/// </para>
		/// </remarks>
		public static void SetVariable(this IDatabaseConnection connection, string name, Expression exp, IQueryContext context) {
			if (name.ToUpper().Equals("ERROR_ON_DIRTY_SELECT")) {
				connection.ErrorOnDirtySelect = ToBooleanValue(exp, context);
			} else if (name.ToUpper().Equals("CASE_INSENSITIVE_IDENTIFIERS")) {
				connection.IsInCaseInsensitiveMode = ToBooleanValue(exp, context);
			} else if (name.ToUpper().Equals("AUTO_COMMIT")) {
				connection.AutoCommit = ToBooleanValue(exp, context);
			} else {
				connection.Transaction.Variables.SetVariable(name, exp, context);
			}
		}

		public static Variable GetVariable(this IDatabaseConnection connection, string name) {
			return connection.Transaction.Variables.GetVariable(name);
		}

		public static void RemoveVariable(this IDatabaseConnection connection, string name) {
			connection.Transaction.Variables.RemoveVariable(name);
		}

		/// <inheritdoc cref="Transactions.Transaction.SetPersistentVariable"/>
		public static void SetPersistentVariable(this IDatabaseConnection connection, string variable, String value) {
			// Assert
			connection.AssertExclusive();
			connection.CommittableTransaction().SetPersistentVariable(variable, value);
		}

		/// <inheritdoc cref="Transactions.Transaction.GetPersistantVariable"/>
		public static string GetPersistentVariable(this IDatabaseConnection connection, string variable) {
			return connection.Transaction.GetPersistantVariable(variable);
		}

		#endregion

		#region Cursors

		/// <summary>
		/// Declares a cursor identified by the given name and on
		/// the specified query.
		/// </summary>
		/// <param name="name">The name of the cursor to create.</param>
		/// <param name="queryPlan">The query used by the cursor to iterate
		/// through the results.</param>
		/// <param name="attributes">The attributes to define a cursor.</param>
		/// <returns>
		/// Returns the newly created <see cref="Cursor"/> instance.
		/// </returns>
		public static Cursor DeclareCursor(this IDatabaseConnection connection, TableName name, IQueryPlanNode queryPlan, CursorAttributes attributes) {
			return connection.CommittableTransaction().DeclareCursor(name, queryPlan, attributes);
		}

		/// <summary>
		/// Declares a scrollable cursor identified by the given name and on
		/// the specified query.
		/// </summary>
		/// <param name="name">The name of the cursor to create.</param>
		/// <param name="queryPlan">The query used by the cursor to iterate
		/// through the results.</param>
		/// <returns>
		/// Returns the newly created <see cref="Cursor"/> instance.
		/// </returns>
		public static Cursor DeclareCursor(this IDatabaseConnection connection, TableName name, IQueryPlanNode queryPlan) {
			return connection.DeclareCursor(name, queryPlan, CursorAttributes.ReadOnly);
		}

		/// <summary>
		/// Gets the instance of a cursor name.
		/// </summary>
		/// <param name="name">The name of the cursor to get.</param>
		/// <returns>
		/// Returns the instance of the <see cref="Cursor"/> identified by
		/// the given name, or <c>null</c> if it was not found.
		/// </returns>
		public static Cursor GetCursor(this IDatabaseConnection connection, TableName name) {
			return connection.Transaction.GetCursor(name);
		}

		public static bool CursorExists(this IDatabaseConnection connection, TableName name) {
			return connection.Transaction.CursorExists(name);
		}

		#endregion
	}
}