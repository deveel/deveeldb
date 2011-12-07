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
		/// <param name="table_name">Name of the table to check.</param>
		/// <remarks>
		/// This method checks if the table exists within the <see cref="CurrentSchema"/>
		/// of the session.
		/// </remarks>
		/// <returns>
		/// Returns <b>true</b> if a table with the given <paramref name="table_name"/>
		/// exists within the underlying transaction, otherwise <b>false</b>.
		/// </returns>
		public bool TableExists(String table_name) {
			return TableExists(new TableName(current_schema, table_name));
		}

		/// <summary>
		/// Checks the existence of a table within the underlying transaction.
		/// </summary>
		/// <param name="table_name">Name of the table to check.</param>
		/// <returns>
		/// Returns <b>true</b> if a table with the given <paramref name="table_name"/>
		/// exists within the underlying transaction, otherwise <b>false</b>.
		/// </returns>
		public bool TableExists(TableName table_name) {
			table_name = SubstituteReservedTableName(table_name);
			return Transaction.TableExists(table_name);
		}

		/// <summary>
		/// Gets the type of he given table.
		/// </summary>
		/// <param name="table_name">Name of the table to get the type.</param>
		/// <remarks>
		/// Currently this is either <i>TABLE</i> or <i>VIEW</i>.
		/// </remarks>
		/// <returns>
		/// Returns a string describing the type of the table identified by the
		/// given <paramref name="table_name"/>.
		/// </returns>
		/// <exception cref="StatementException">
		/// If none table with the given <paramref name="table_name"/> was found 
		/// in the underlying transaction.</exception>
		public String GetTableType(TableName table_name) {
			table_name = SubstituteReservedTableName(table_name);
			return Transaction.GetTableType(table_name);
		}

		/// <summary>
		/// Attempts to resolve the given table name to its correct case assuming
		/// the table name represents a case insensitive version of the name.
		/// </summary>
		/// <param name="table_name">Table name to resolve.</param>
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
		/// resolve the given <paramref name="table_name"/>, otherwise returns
		/// the input table name.
		/// </returns>
		public TableName TryResolveCase(TableName table_name) {
			table_name = SubstituteReservedTableName(table_name);
			table_name = Transaction.TryResolveCase(table_name);
			return table_name;
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
		public TableName ResolveTableName(String name) {
			TableName table_name = TableName.Resolve(CurrentSchema, name);
			table_name = SubstituteReservedTableName(table_name);
			if (IsInCaseInsensitiveMode) {
				// Try and resolve the case of the table name,
				table_name = TryResolveCase(table_name);
			}
			return table_name;
		}

		/// <summary>
		/// Resolves the given string to a table name
		/// </summary>
		/// <param name="name">Table name to resolve.</param>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If the reference is ambigous or
		/// </exception>
		public TableName ResolveToTableName(String name) {
			TableName table_name = TableName.Resolve(CurrentSchema, name);
			if (String.Compare(table_name.Name, "OLD", true) == 0) {
				return Database.OldTriggerTable;
			} else if (String.Compare(table_name.Name, "NEW", true) == 0) {
				return Database.NewTriggerTable;
			}

			return Transaction.ResolveToTableName(CurrentSchema, name, IsInCaseInsensitiveMode);

		}

		/// <summary>
		/// Gets the meta informations for the given table.
		/// </summary>
		/// <param name="name">Name of the table to return the 
		/// meta informations.</param>
		/// <returns>
		/// Returns the <see cref="DataTableDef"/> representing the meta 
		/// informations for the tabl identified by <paramref name="name"/> 
		/// if found, otherwise <b>null</b>.
		/// </returns>
		public DataTableDef GetDataTableDef(TableName name) {
			name = SubstituteReservedTableName(name);
			return Transaction.GetDataTableDef(name);
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
					if (currentOldNewState.OLD_data_table == null) {
						currentOldNewState.OLD_data_table =
										new DataTable(this, Transaction.GetTable(name));
					}
					return currentOldNewState.OLD_data_table;
				} else if (name.Equals(Database.NewTriggerTable)) {
					if (currentOldNewState.NEW_data_table == null) {
						currentOldNewState.NEW_data_table =
										new DataTable(this, Transaction.GetTable(name));
					}
					return currentOldNewState.NEW_data_table;
				}

				// Ask the transaction for the table
				IMutableTableDataSource table = Transaction.GetTable(name);

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
		/// <param name="table_name">Name of the table to return.</param>
		/// <remarks>
		/// This method uses the <see cref="CurrentSchema"/> to get the table.
		/// </remarks>
		/// <returns>
		/// Returns a <see cref="DataTable"/> that represents the table 
		/// identified by <paramref name="table_name"/>, otherwise returns <b>null</b>.
		/// </returns>
		/// <exception cref="DatabaseException">
		/// If none table was found for the given <paramref name="table_name"/>.
		/// </exception>
		public DataTable GetTable(String table_name) {
			return GetTable(new TableName(current_schema, table_name));
		}

		/// <summary>
		/// Creates a new table within the context of the transaction.
		/// </summary>
		/// <param name="table_def">Table meta informations for creating the table.</param>
		/// <exception cref="StatementException">
		/// If the name of the table is reserved and the creation of the table 
		/// should be prevented.
		/// </exception>
		public void CreateTable(DataTableDef table_def) {
			CheckAllowCreate(table_def.TableName);
			Transaction.CreateTable(table_def);
		}

		/// <summary>
		/// Creates a new table within this transaction with the given 
		/// sector size.
		/// </summary>
		/// <param name="table_def">Meta informations used to create the table.</param>
		/// <param name="data_sector_size">Size of data sectors of the table.</param>
		/// <param name="index_sector_size">Size of the index sectors of the table.</param>
		/// <remarks>
		/// This should only be used as very fine grain optimization for 
		/// creating tables. 
		/// If in the future the underlying table model is changed so that the given
		/// <paramref name="data_sector_size"/> value is unapplicable, then the value 
		/// will be ignored.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If a table with the same name (specified by <paramref name="table_def"/>) 
		/// already exists.
		/// </exception>
		public void CreateTable(DataTableDef table_def, int data_sector_size, int index_sector_size) {
			CheckAllowCreate(table_def.TableName);
			Transaction.CreateTable(table_def, data_sector_size, index_sector_size);
		}

		/// <summary>
		/// Alters a table within the underlying transaction.
		/// </summary>
		/// <param name="table_def">Table metadata informations for aletring the table</param>
		/// <exception cref="StatementException">
		/// If the name of the table is reserved and the creation of the table 
		/// should be prevented.
		/// </exception>
		public void UpdateTable(DataTableDef table_def) {
			CheckAllowCreate(table_def.TableName);
			Transaction.AlterTable(table_def.TableName, table_def);
		}

		/// <summary>
		/// Alters a table within the underlying transaction.
		/// </summary>
		/// <param name="table_def">Table metadata informations for altering 
		/// the table.</param>
		/// <param name="data_sector_size"></param>
		/// <param name="index_sector_size"></param>
		/// <remarks>
		/// This should only be used as very fine grain optimization
		/// for creating tables. If in the future the underlying table model is
		/// changed so that the given <paramref name="data_sector_size"/> value 
		/// is unapplicable, then the value will be ignored.
		/// </remarks>
		public void UpdateTable(DataTableDef table_def, int data_sector_size, int index_sector_size) {
			CheckAllowCreate(table_def.TableName);
			Transaction.AlterTable(table_def.TableName, table_def, data_sector_size, index_sector_size);
		}

		/// <summary>
		/// If a table exists with the given table name (defined by <paramref name="table_def"/>)
		/// alters its the structure, otherwise creates a new table.
		/// </summary>
		/// <param name="table_def">Meta informations for altering or creating a table.</param>
		/// <param name="data_sector_size">Size of data sectors of the table.</param>
		/// <param name="index_sector_size">Size of the index sectors of the table.</param>
		/// <remarks>
		/// This should only be used as very fine grain optimization for creating or
		/// altering tables.
		/// If in the future the underlying table model is changed so that the given 
		/// <paramref name="data_sector_size"/> and <paramref name="index_sector_size"/> 
		/// values are unapplicable and will be ignored.
		/// </remarks>
		public void AlterCreateTable(DataTableDef table_def, int data_sector_size, int index_sector_size) {
			if (!TableExists(table_def.TableName)) {
				CreateTable(table_def, data_sector_size, index_sector_size);
			} else {
				UpdateTable(table_def, data_sector_size, index_sector_size);
			}
		}

		/// <summary>
		/// If a table exists with the given table name (defined by <paramref name="table_def"/>)
		/// alters its the structure, otherwise creates a new table.
		/// </summary>
		/// <param name="table_def">Meta informations for altering or creating a table.</param>
		/// <exception cref="StatementException"></exception>
		public void AlterCreateTable(DataTableDef table_def) {
			if (!TableExists(table_def.TableName)) {
				CreateTable(table_def);
			} else {
				UpdateTable(table_def);
			}
		}

		/// <summary>
		/// Drops a table within the transaction.
		/// </summary>
		/// <param name="table_name">Name of the table to drop.</param>
		/// <exception cref="TransactionException">
		/// If none tables was found for the given <paramref name="table_name"/>.
		/// </exception>
		public void DropTable(String table_name) {
			DropTable(new TableName(current_schema, table_name));
		}

		/// <summary>
		/// Drops a table within the transaction.
		/// </summary>
		/// <param name="table_name">Name of the table to drop.</param>
		/// <exception cref="TransactionException">
		/// If none tables was found for the given <paramref name="table_name"/>.
		/// </exception>
		public void DropTable(TableName table_name) {
			Transaction.DropTable(table_name);
		}

		/// <summary>
		/// Compacts a table with the given name.
		/// </summary>
		/// <param name="table_name">The name of the table to compact.</param>
		/// <exception cref="StatementException">
		/// If none table was found for the given <paramref name="table_name"/> 
		/// in the <see cref="CurrentSchema"/>.
		/// </exception>
		public void CompactTable(String table_name) {
			CompactTable(new TableName(current_schema, table_name));
		}

		/// <summary>
		/// Compacts a table with the given name.
		/// </summary>
		/// <param name="table_name">The name of the table to compact.</param>
		/// <exception cref="StatementException">
		/// If none table was found for the given <paramref name="table_name"/>.
		/// </exception>
		public void CompactTable(TableName table_name) {
			Transaction.CompactTable(table_name);
		}

		///<summary>
		/// Adds the given table name to the list of tables that are selected from
		/// within the transaction in this connection.
		///</summary>
		///<param name="table_name"></param>
		public void AddSelectedFromTable(String table_name) {
			AddSelectedFromTable(new TableName(current_schema, table_name));
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