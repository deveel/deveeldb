// 
//  Copyright 2010-2011  Deveel
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
using System.Collections.Generic;

namespace Deveel.Data {
	public sealed partial class DatabaseConnection {
		/// <summary>
		/// A Hashtable of DataTable objects that have been created within this connection.
		/// </summary>
		private readonly Dictionary<TableName, DataTable> tablesCache = new Dictionary<TableName, DataTable>();

		/// <summary>
		/// Gets an array of <see cref="TableName"/> that contains the 
		/// list of database tables visible by the underlying transaction.
		/// </summary>
		/// <remarks>
		/// The list returned represents all the queriable tables in
		/// the database.
		/// </remarks>
		public TableName[] Tables {
			get { return Transaction.GetTables(); }
		}

		/// <summary>
		/// Checks the existence of a table within the underlying transaction.
		/// </summary>
		/// <param name="tableName">Name of the table to check.</param>
		/// <remarks>
		/// This method checks if the table exists within the <see cref="CurrentSchema"/>
		/// of the session.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if a table with the given <paramref name="tableName"/>
		/// exists within the underlying transaction, otherwise <b>false</b>.
		/// </returns>
		public bool TableExists(string tableName) {
			return TableExists(new TableName(current_schema, tableName));
		}

		/// <summary>
		/// Checks the existence of a table within the underlying transaction.
		/// </summary>
		/// <param name="tableName">Name of the table to check.</param>
		/// <returns>
		/// Returns <b>true</b> if a table with the given <paramref name="tableName"/>
		/// exists within the underlying transaction, otherwise <b>false</b>.
		/// </returns>
		public bool TableExists(TableName tableName) {
			tableName = SubstituteReservedTableName(tableName);
			return Transaction.TableExists(tableName);
		}

		/// <summary>
		/// Gets the type of he given table.
		/// </summary>
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
		public string GetTableType(TableName tableName) {
			tableName = SubstituteReservedTableName(tableName);
			return Transaction.GetTableType(tableName);
		}

		/// <summary>
		/// Attempts to resolve the given table name to its correct case assuming
		/// the table name represents a case insensitive version of the name.
		/// </summary>
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
		public TableName TryResolveCase(TableName tableName) {
			tableName = SubstituteReservedTableName(tableName);
			tableName = Transaction.TryResolveCase(tableName);
			return tableName;
		}

		/// <summary>
		/// Resolves a table name.
		/// </summary>
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
		public TableName ResolveTableName(string name) {
			TableName tableName = TableName.Resolve(CurrentSchema, name);
			tableName = SubstituteReservedTableName(tableName);
			if (IsInCaseInsensitiveMode) {
				// Try and resolve the case of the table name,
				tableName = TryResolveCase(tableName);
			}
			return tableName;
		}

		/// <summary>
		/// Resolves the given string to a table name
		/// </summary>
		/// <param name="name">Table name to resolve.</param>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If the reference is ambigous or
		/// </exception>
		public TableName ResolveToTableName(string name) {
			TableName table_name = TableName.Resolve(CurrentSchema, name);
			if (String.Compare(table_name.Name, "OLD", true) == 0)
				return Database.OldTriggerTable;
			if (String.Compare(table_name.Name, "NEW", true) == 0)
				return Database.NewTriggerTable;

			return Transaction.ResolveToTableName(CurrentSchema, name, IsInCaseInsensitiveMode);

		}

		/// <summary>
		/// Gets the meta informations for the given table.
		/// </summary>
		/// <param name="name">Name of the table to return the 
		/// meta informations.</param>
		/// <returns>
		/// Returns the <see cref="DataTableInfo"/> representing the meta 
		/// informations for the tabl identified by <paramref name="name"/> 
		/// if found, otherwise <b>null</b>.
		/// </returns>
		public DataTableInfo GetTableInfo(TableName name) {
			name = SubstituteReservedTableName(name);
			return Transaction.GetTableInfo(name);
		}

		/// <summary>
		/// Gets the table for the given name.
		/// </summary>
		/// <param name="name">Name of the table to return.</param>
		/// <returns>
		/// Returns a <see cref="DataTable"/> that represents the table 
		/// identified by <paramref name="name"/>, otherwise returns 
		/// <b>null</b>.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If none table was found for the given <paramref name="name"/>.
		/// </exception>
		public DataTable GetTable(TableName name) {
			name = SubstituteReservedTableName(name);

			try {
				// Special handling of NEW and OLD table, we cache the DataTable in the
				// OldNewTableState object,
				if (name.Equals(Database.OldTriggerTable)) {
					if (currentOldNewState.OldDataTable == null)
						currentOldNewState.OldDataTable = new DataTable(this, Transaction.GetTable(name));
					return currentOldNewState.OldDataTable;
				}
				if (name.Equals(Database.NewTriggerTable)) {
					if (currentOldNewState.NewDataTable == null)
						currentOldNewState.NewDataTable = new DataTable(this, Transaction.GetTable(name));
					return currentOldNewState.NewDataTable;
				}

				// Ask the transaction for the table
				ITableDataSource table = Transaction.GetTable(name);

				// Is this table in the tables_cache?
				DataTable dtable;
				if (!tablesCache.TryGetValue(name, out dtable)) {
					// No, so wrap it around a Datatable and WriteByte it in the cache
					dtable = new DataTable(this, table);
					tablesCache[name] = dtable;
				}

				// Return the DataTable
				return dtable;

			} catch (DatabaseException e) {
				Debug.WriteException(e);
				throw new ApplicationException("Database Exception: " + e.Message);
			}

		}

		/// <summary>
		/// Gets the table for the given name.
		/// </summary>
		/// <param name="tableName">Name of the table to return.</param>
		/// <remarks>
		/// This method uses the <see cref="CurrentSchema"/> to get the table.
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="DataTable"/> that represents the table 
		/// identified by <paramref name="tableName"/>, otherwise returns <b>null</b>.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If none table was found for the given <paramref name="tableName"/>.
		/// </exception>
		public DataTable GetTable(string tableName) {
			return GetTable(new TableName(current_schema, tableName));
		}

		/// <summary>
		/// Creates a new table within the context of the transaction.
		/// </summary>
		/// <param name="tableInfo">Table meta informations for creating the table.</param>
		/// <exception cref="StatementException">
		/// If the name of the table is reserved and the creation of the table 
		/// should be prevented.
		/// </exception>
		public void CreateTable(DataTableInfo tableInfo) {
			CheckAllowCreate(tableInfo.TableName);
			Transaction.CreateTable(tableInfo);
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
		public void CreateTable(DataTableInfo tableInfo, int dataSectorSize, int indexSectorSize) {
			CheckAllowCreate(tableInfo.TableName);
			Transaction.CreateTable(tableInfo, dataSectorSize, indexSectorSize);
		}

		/// <summary>
		/// Alters a table within the underlying transaction.
		/// </summary>
		/// <param name="tableInfo">Table metadata informations for aletring the table</param>
		/// <exception cref="StatementException">
		/// If the name of the table is reserved and the creation of the table 
		/// should be prevented.
		/// </exception>
		public void UpdateTable(DataTableInfo tableInfo) {
			CheckAllowCreate(tableInfo.TableName);
			Transaction.AlterTable(tableInfo.TableName, tableInfo);
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
		public void UpdateTable(DataTableInfo tableInfo, int dataSectorSize, int indexSectorSize) {
			CheckAllowCreate(tableInfo.TableName);
			Transaction.AlterTable(tableInfo.TableName, tableInfo, dataSectorSize, indexSectorSize);
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
		public void AlterCreateTable(DataTableInfo tableInfo, int dataSectorSize, int indexSectorSize) {
			if (!TableExists(tableInfo.TableName)) {
				CreateTable(tableInfo, dataSectorSize, indexSectorSize);
			} else {
				UpdateTable(tableInfo, dataSectorSize, indexSectorSize);
			}
		}

		/// <summary>
		/// If a table exists with the given table name (defined by <paramref name="tableInfo"/>)
		/// alters its the structure, otherwise creates a new table.
		/// </summary>
		/// <param name="tableInfo">Meta informations for altering or creating a table.</param>
		/// <exception cref="StatementException"></exception>
		public void AlterCreateTable(DataTableInfo tableInfo) {
			if (!TableExists(tableInfo.TableName)) {
				CreateTable(tableInfo);
			} else {
				UpdateTable(tableInfo);
			}
		}

		/// <summary>
		/// Drops a table within the transaction.
		/// </summary>
		/// <param name="tableName">Name of the table to drop.</param>
		/// <exception cref="TransactionException">
		/// If none tables was found for the given <paramref name="tableName"/>.
		/// </exception>
		public void DropTable(string tableName) {
			DropTable(new TableName(current_schema, tableName));
		}

		/// <summary>
		/// Drops a table within the transaction.
		/// </summary>
		/// <param name="tableName">Name of the table to drop.</param>
		/// <exception cref="TransactionException">
		/// If none tables was found for the given <paramref name="tableName"/>.
		/// </exception>
		public void DropTable(TableName tableName) {
			Transaction.DropTable(tableName);
		}

		/// <summary>
		/// Compacts a table with the given name.
		/// </summary>
		/// <param name="tableName">The name of the table to compact.</param>
		/// <exception cref="StatementException">
		/// If none table was found for the given <paramref name="tableName"/> 
		/// in the <see cref="CurrentSchema"/>.
		/// </exception>
		public void CompactTable(string tableName) {
			CompactTable(new TableName(current_schema, tableName));
		}

		/// <summary>
		/// Compacts a table with the given name.
		/// </summary>
		/// <param name="tableName">The name of the table to compact.</param>
		/// <exception cref="StatementException">
		/// If none table was found for the given <paramref name="tableName"/>.
		/// </exception>
		public void CompactTable(TableName tableName) {
			Transaction.CompactTable(tableName);
		}

		///<summary>
		/// Adds the given table name to the list of tables that are selected from
		/// within the transaction in this connection.
		///</summary>
		///<param name="tableName"></param>
		public void AddSelectedFromTable(string tableName) {
			AddSelectedFromTable(new TableName(current_schema, tableName));
		}

		///<summary>
		/// Adds the given table name to the list of tables that are selected from
		/// within the transaction in this connection.
		///</summary>
		///<param name="name"></param>
		public void AddSelectedFromTable(TableName name) {
			Transaction.AddSelectedFromTable(name);
		}
	}
}