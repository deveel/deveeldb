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
using System.Collections.Generic;

using Deveel.Diagnostics;

namespace Deveel.Data {
	public partial class Transaction {
		// ----- Setting/Querying constraint information -----
		// PENDING: Is it worth implementing a pluggable constraint architecture
		//   as described in the idea below.  With the current implementation we
		//   have tied a DataTableConglomerate to a specific constraint
		//   architecture.
		//
		// IDEA: These methods delegate to the parent conglomerate which has a
		//   pluggable architecture for setting/querying constraints.  Some uses of
		//   a conglomerate may not need integrity constraints or may implement the
		//   mechanism for storing/querying in a different way.  This provides a
		//   useful abstraction of being enable to implement constraint behaviour
		//   by only providing a way to set/query the constraint information in
		//   different conglomerate uses.

		/// <summary>
		/// Generates a unique constraint name.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="uniqueId"></param>
		/// <remarks>
		/// If the given constraint name is 'null' then a new one is created, 
		/// otherwise the given default one is returned.
		/// </remarks>
		/// <returns></returns>
		private static string MakeUniqueConstraintName(string name, BigNumber uniqueId) {
			return name ?? ("_ANONYMOUS_CONSTRAINT_" + uniqueId);
		}

		/// <summary>
		/// Adds a unique constraint to the database which becomes perminant 
		/// when the transaction is committed.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="cols"></param>
		/// <param name="deferred"></param>
		/// <param name="constraintName"></param>
		/// <remarks>
		/// Columns in a table that are defined as unique are prevented from 
		/// being duplicated by the engine.
		/// <para>
		/// <b>Note</b> Security checks for adding constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		public void AddUniqueConstraint(TableName tableName, string[] cols, ConstraintDeferrability deferred, string constraintName) {
			TableName tn1 = TableDataConglomerate.UniqueInfoTable;
			TableName tn2 = TableDataConglomerate.UniqueColsTable;
			IMutableTableDataSource t = GetMutableTable(tn1);
			IMutableTableDataSource tcols = GetMutableTable(tn2);

			try {

				// Insert a value into UniqueInfoTable
				DataRow row = new DataRow(t);
				BigNumber uniqueId = NextUniqueID(tn1);
				constraintName = MakeUniqueConstraintName(constraintName, uniqueId);
				row.SetValue(0, uniqueId);
				row.SetValue(1, constraintName);
				row.SetValue(2, tableName.Schema);
				row.SetValue(3, tableName.Name);
				row.SetValue(4, (BigNumber)((short)deferred));
				t.AddRow(row);

				// Insert the columns
				for (int i = 0; i < cols.Length; ++i) {
					row = new DataRow(tcols);
					row.SetValue(0, uniqueId);            // unique id
					row.SetValue(1, cols[i]);              // column name
					row.SetValue(2, (BigNumber)i);         // sequence number
					tcols.AddRow(row);
				}

			} catch (DatabaseConstraintViolationException e) {
				// Constraint violation when inserting the data.  Check the type and
				// wrap around an appropriate error message.
				if (e.ErrorCode == DatabaseConstraintViolationException.UniqueViolation)
					// This means we gave a constraint name that's already being used
					// for a primary key.
					throw new StatementException("Unique constraint name '" + constraintName + "' is already being used.");

				throw;
			}

		}

		/// <summary>
		/// Adds a foreign key constraint to the database which becomes perminent
		/// when the transaction is committed.
		/// </summary>
		/// <param name="table">The key table to link from.</param>
		/// <param name="cols">The key columns to link from</param>
		/// <param name="refTable">The referenced table to link to.</param>
		/// <param name="refCols">The refenced columns to link to.</param>
		/// <param name="deleteRule">The rule called during cascade delete.</param>
		/// <param name="updateRule">The rule called during cascade update.</param>
		/// <param name="deferred"></param>
		/// <param name="constraintName">The name of the constraint to create.</param>
		/// <remarks>
		/// A foreign key represents a referential link from one table to 
		/// another (may be the same table).
		/// <para>
		/// <b>Note</b> Security checks for adding constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		public void AddForeignKeyConstraint(TableName table, string[] cols, 
			TableName refTable, string[] refCols, 
			ConstraintAction deleteRule, ConstraintAction updateRule, ConstraintDeferrability deferred, String constraintName) {
			TableName tn1 = TableDataConglomerate.ForeignInfoTable;
			TableName tn2 = TableDataConglomerate.ForeignColsTable;
			IMutableTableDataSource t = GetMutableTable(tn1);
			IMutableTableDataSource tcols = GetMutableTable(tn2);

			try {
				// If 'ref_columns' empty then set to primary key for referenced table,
				// ISSUE: What if primary key changes after the fact?
				if (refCols.Length == 0) {
					ColumnGroup set = QueryTablePrimaryKeyGroup(this, refTable);
					if (set == null)
						throw new StatementException("No primary key defined for referenced table '" + refTable + "'");

					refCols = set.columns;
				}

				if (cols.Length != refCols.Length) {
					throw new StatementException("Foreign key reference '" + table +
					  "' -> '" + refTable + "' does not have an equal number of " +
					  "column terms.");
				}

				// If delete or update rule is 'SET NULL' then check the foreign key
				// columns are not constrained as 'NOT NULL'
				if (deleteRule == ConstraintAction.SetNull ||
					updateRule == ConstraintAction.SetNull) {
					DataTableInfo tableInfo = GetTableInfo(table);
					for (int i = 0; i < cols.Length; ++i) {
						DataTableColumnInfo columnInfo = tableInfo[tableInfo.FindColumnName(cols[i])];
						if (columnInfo.IsNotNull) {
							throw new StatementException("Foreign key reference '" + table +
								   "' -> '" + refTable + "' update or delete triggered " +
								   "action is SET NULL for columns that are constrained as " +
								   "NOT NULL.");
						}
					}
				}

				// Insert a value into ForeignInfoTable
				DataRow row = new DataRow(t);
				BigNumber uniqueId = NextUniqueID(tn1);
				constraintName = MakeUniqueConstraintName(constraintName, uniqueId);
				row.SetValue(0, uniqueId);
				row.SetValue(1, constraintName);
				row.SetValue(2, table.Schema);
				row.SetValue(3, table.Name);
				row.SetValue(4, refTable.Schema);
				row.SetValue(5, refTable.Name);
				row.SetValue(6, (BigNumber)((int)updateRule));
				row.SetValue(7, (BigNumber)((int)deleteRule));
				row.SetValue(8, (BigNumber)((short)deferred));
				t.AddRow(row);

				// Insert the columns
				for (int i = 0; i < cols.Length; ++i) {
					row = new DataRow(tcols);
					row.SetValue(0, uniqueId);            // unique id
					row.SetValue(1, cols[i]);              // column name
					row.SetValue(2, refCols[i]);          // ref column name
					row.SetValue(3, (BigNumber)i); // sequence number
					tcols.AddRow(row);
				}

			} catch (DatabaseConstraintViolationException e) {
				// Constraint violation when inserting the data.  Check the type and
				// wrap around an appropriate error message.
				if (e.ErrorCode == DatabaseConstraintViolationException.UniqueViolation)
					// This means we gave a constraint name that's already being used
					// for a primary key.
					throw new StatementException("Foreign key constraint name '" + constraintName + "' is already being used.");

				throw;
			}
		}

		/// <summary>
		/// Adds a primary key constraint that becomes perminent when the 
		/// transaction is committed.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="cols"></param>
		/// <param name="deferred"></param>
		/// <param name="constraintName"></param>
		/// <remarks>
		/// A primary key represents a set of columns in a table that are 
		/// constrained to be unique and can not be null. If the constraint 
		/// name parameter is 'null' a primary key constraint is created with 
		/// a unique constraint name.
		/// <para>
		/// <b>Note</b> Security checks for adding constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		public void AddPrimaryKeyConstraint(TableName tableName, string[] cols, ConstraintDeferrability deferred, string constraintName) {
			TableName tn1 = TableDataConglomerate.PrimaryInfoTable;
			TableName tn2 = TableDataConglomerate.PrimaryColsTable;
			IMutableTableDataSource t = GetMutableTable(tn1);
			IMutableTableDataSource tcols = GetMutableTable(tn2);

			try {
				// Insert a value into PrimaryInfoTable
				DataRow row = new DataRow(t);
				BigNumber unique_id = NextUniqueID(tn1);
				constraintName = MakeUniqueConstraintName(constraintName, unique_id);
				row.SetValue(0, unique_id);
				row.SetValue(1, constraintName);
				row.SetValue(2, tableName.Schema);
				row.SetValue(3, tableName.Name);
				row.SetValue(4, (BigNumber)((short)deferred));
				t.AddRow(row);

				// Insert the columns
				for (int i = 0; i < cols.Length; ++i) {
					row = new DataRow(tcols);
					row.SetValue(0, unique_id);            // unique id
					row.SetValue(1, cols[i]);              // column name
					row.SetValue(2, (BigNumber)i);         // Sequence number
					tcols.AddRow(row);
				}

			} catch (DatabaseConstraintViolationException e) {
				// Constraint violation when inserting the data.  Check the type and
				// wrap around an appropriate error message.
				if (e.ErrorCode == DatabaseConstraintViolationException.UniqueViolation) {
					// This means we gave a constraint name that's already being used
					// for a primary key.
					throw new StatementException("Primary key constraint name '" +
										   constraintName + "' is already being used.");
				}

				throw;
			}
		}

		/// <summary>
		/// Adds a check expression that becomes perminent when the transaction
		/// is committed.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="expression"></param>
		/// <param name="deferred"></param>
		/// <param name="constraintName"></param>
		/// <remarks>
		/// A check expression is an expression that must evaluate to true 
		/// for all records added/updated in the database.
		/// <para>
		/// <b>Note</b> Security checks for adding constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		public void AddCheckConstraint(TableName tableName, Expression expression, ConstraintDeferrability deferred, string constraintName) {
			TableName tn = TableDataConglomerate.CheckInfoTable;
			IMutableTableDataSource t = GetMutableTable(tn);
			int colCount = t.TableInfo.ColumnCount;

			try {
				// Insert check constraint data.
				BigNumber uniqueId = NextUniqueID(tn);
				constraintName = MakeUniqueConstraintName(constraintName, uniqueId);
				DataRow rd = new DataRow(t);
				rd.SetValue(0, uniqueId);
				rd.SetValue(1, constraintName);
				rd.SetValue(2, tableName.Schema);
				rd.SetValue(3, tableName.Name);
				rd.SetValue(4, expression.Text.ToString());
				rd.SetValue(5, (BigNumber)((short)deferred));
				if (colCount > 6) {
					// Serialize the check expression
					ByteLongObject serializedExpression = ObjectTranslator.Serialize(expression);
					rd.SetValue(6, serializedExpression);
				}
				t.AddRow(rd);

			} catch (DatabaseConstraintViolationException e) {
				// Constraint violation when inserting the data.  Check the type and
				// wrap around an appropriate error message.
				if (e.ErrorCode == DatabaseConstraintViolationException.UniqueViolation) {
					// This means we gave a constraint name that's already being used.
					throw new StatementException("Check constraint name '" +
										   constraintName + "' is already being used.");
				}
				throw;
			}
		}

		/// <summary>
		/// Drops all the constraints defined for the given table.
		/// </summary>
		/// <param name="tableName"></param>
		/// <remarks>
		/// This is a useful function when dropping a table from the database.
		/// <para>
		/// <b>Note</b> Security checks for dropping constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		public void DropAllConstraintsForTable(TableName tableName) {
			ColumnGroup primary = QueryTablePrimaryKeyGroup(this, tableName);
			ColumnGroup[] uniques = QueryTableUniqueGroups(this, tableName);
			CheckExpression[] expressions = QueryTableCheckExpressions(this, tableName);
			ColumnGroupReference[] refs = QueryTableForeignKeyReferences(this, tableName);

			if (primary != null) {
				DropPrimaryKeyConstraintForTable(tableName, primary.name);
			}
			for (int i = 0; i < uniques.Length; ++i) {
				DropUniqueConstraintForTable(tableName, uniques[i].name);
			}
			for (int i = 0; i < expressions.Length; ++i) {
				DropCheckConstraintForTable(tableName, expressions[i].name);
			}
			for (int i = 0; i < refs.Length; ++i) {
				DropForeignKeyReferenceConstraintForTable(tableName, refs[i].name);
			}
		}

		/// <summary>
		/// Drops the named constraint from the transaction.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="constraintName"></param>
		/// <remarks>
		/// Used when altering table schema. Returns the number of constraints 
		/// that were removed from the system. If this method returns 0 then 
		/// it indicates there is no constraint with the given name in the 
		/// table.
		/// <para>
		/// <b>Note</b> Security checks for dropping constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns the actual count of dropped constraints.
		/// </returns>
		public int DropNamedConstraint(TableName tableName, string constraintName) {
			int dropCount = 0;
			if (DropPrimaryKeyConstraintForTable(tableName, constraintName)) {
				++dropCount;
			}
			if (DropUniqueConstraintForTable(tableName, constraintName)) {
				++dropCount;
			}
			if (DropCheckConstraintForTable(tableName, constraintName)) {
				++dropCount;
			}
			if (DropForeignKeyReferenceConstraintForTable(tableName,
														  constraintName)) {
				++dropCount;
			}
			return dropCount;
		}

		/// <summary>
		/// Drops the primary key constraint for the given table.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="constraintName"></param>
		/// <remarks>
		/// Used when altering table schema. If 'constraint_name' is null this 
		/// method will search for the primary key of the table name. 
		/// <para>
		/// <b>Note</b> Security checks for dropping constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns true if the primary key constraint was dropped (the 
		/// constraint existed), otherwise false.
		/// </returns>
		public bool DropPrimaryKeyConstraintForTable(TableName tableName, string constraintName) {
			IMutableTableDataSource t = GetMutableTable(TableDataConglomerate.PrimaryInfoTable);
			IMutableTableDataSource t2 = GetMutableTable(TableDataConglomerate.PrimaryColsTable);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			try {
				IList<int> data;
				if (constraintName != null) {
					// Returns the list of indexes where column 1 = constraint name
					//                               and column 2 = schema name
					data = dt.SelectEqual(1, constraintName, 2, tableName.Schema);
				} else {
					// Returns the list of indexes where column 3 = table name
					//                               and column 2 = schema name
					data = dt.SelectEqual(3, tableName.Name, 2, tableName.Schema);
				}

				if (data.Count > 1) {
					throw new ApplicationException("Assertion failed: multiple primary key for: " + tableName);
				} else if (data.Count == 1) {
					int rowIndex = data[0];
					// The id
					TObject id = dt.Get(0, rowIndex);
					// All columns with this id
					IList<int> ivec = dtcols.SelectEqual(0, id);
					// Delete from the table
					dtcols.DeleteRows(ivec);
					dt.DeleteRows(data);
					return true;
				}
				// data.size() must be 0 so no constraint was found to drop.
				return false;
			} finally {
				dtcols.Dispose();
				dt.Dispose();
			}
		}

		/// <summary>
		/// Drops a single named unique constraint from the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="constraintName"></param>
		/// <remarks>
		/// <para>
		/// <b>Note</b> Security checks for dropping constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns true if the unique constraint was dropped (the constraint 
		/// existed), otherwise false.
		/// </returns>
		public bool DropUniqueConstraintForTable(TableName table, string constraintName) {
			IMutableTableDataSource t = GetMutableTable(TableDataConglomerate.UniqueInfoTable);
			IMutableTableDataSource t2 = GetMutableTable(TableDataConglomerate.UniqueColsTable);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			try {
				// Returns the list of indexes where column 1 = constraint name
				//                               and column 2 = schema name
				IList<int> data = dt.SelectEqual(1, constraintName,
				                                    2, table.Schema);

				if (data.Count > 1) {
					throw new ApplicationException("Assertion failed: multiple unique constraint name: " + constraintName);
				} else if (data.Count == 1) {
					int rowIndex = data[0];
					// The id
					TObject id = dt.Get(0, rowIndex);
					// All columns with this id
					IList<int> ivec = dtcols.SelectEqual(0, id);
					// Delete from the table
					dtcols.DeleteRows(ivec);
					dt.DeleteRows(data);
					return true;
				}
				// data.size() == 0 so the constraint wasn't found
				return false;
			} finally {
				dtcols.Dispose();
				dt.Dispose();
			}
		}

		/// <summary>
		/// Drops a single named check constraint from the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="constraintName"></param>
		/// <remarks>
		/// <para>
		/// <b>Note</b> Security checks for dropping constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns true if the check constraint was dropped (the constraint 
		/// existed), otherwise false.
		/// </returns>
		public bool DropCheckConstraintForTable(TableName table, string constraintName) {
			IMutableTableDataSource t = GetMutableTable(TableDataConglomerate.CheckInfoTable);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table

			try {
				// Returns the list of indexes where column 1 = constraint name
				//                               and column 2 = schema name
				IList<int> data = dt.SelectEqual(1, constraintName,
				                                    2, table.Schema);

				if (data.Count > 1) {
					throw new ApplicationException("Assertion failed: multiple check constraint name: " + constraintName);
				} else if (data.Count == 1) {
					// Delete the check constraint
					dt.DeleteRows(data);
					return true;
				}
				// data.size() == 0 so the constraint wasn't found
				return false;
			} finally {
				dt.Dispose();
			}
		}

		/// <summary>
		/// Drops a single named foreign key reference from the given table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="constraintName"></param>
		/// <remarks>
		/// <para>
		/// <b>Note</b> Security checks for dropping constraints must be checked 
		/// for at a higher layer.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns true if the foreign key reference constraint was dropped 
		/// (the constraint existed), otherwise false.
		/// </returns>
		public bool DropForeignKeyReferenceConstraintForTable(TableName table, string constraintName) {
			IMutableTableDataSource t = GetMutableTable(TableDataConglomerate.ForeignInfoTable);
			IMutableTableDataSource t2 = GetMutableTable(TableDataConglomerate.ForeignColsTable);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			try {
				// Returns the list of indexes where column 1 = constraint name
				//                               and column 2 = schema name
				IList<int> data = dt.SelectEqual(1, constraintName,
				                                    2, table.Schema);

				if (data.Count > 1) {
					throw new ApplicationException("Assertion failed: multiple foreign key constraint " + "name: " + constraintName);
				} else if (data.Count == 1) {
					int rowIndex = data[0];
					// The id
					TObject id = dt.Get(0, rowIndex);
					// All columns with this id
					IList<int> ivec = dtcols.SelectEqual(0, id);
					// Delete from the table
					dtcols.DeleteRows(ivec);
					dt.DeleteRows(data);
					return true;
				}
				// data.size() == 0 so the constraint wasn't found
				return false;
			} finally {
				dtcols.Dispose();
				dt.Dispose();
			}

		}

		/// <summary>
		/// Returns the list of tables (as a TableName array) that are dependant
		/// on the data in the given table to maintain referential consistancy.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="table"></param>
		/// <remarks>
		/// The list includes the tables referenced as foreign keys, and the 
		/// tables that reference the table as a foreign key.
		/// <para>
		/// This is a useful query for determining ahead of time the tables 
		/// that require a read lock when inserting/updating a table. A table
		/// will require a read lock if the operation needs to query it for 
		/// potential referential integrity violations.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		public static TableName[] QueryTablesRelationallyLinkedTo(SimpleTransaction transaction, TableName table) {
			List<TableName> list = new List<TableName>();
			ColumnGroupReference[] refs = QueryTableForeignKeyReferences(transaction, table);
			for (int i = 0; i < refs.Length; ++i) {
				TableName tname = refs[i].ref_table_name;
				if (!list.Contains(tname)) {
					list.Add(tname);
				}
			}
			refs = QueryTableImportedForeignKeyReferences(transaction, table);
			for (int i = 0; i < refs.Length; ++i) {
				TableName tname = refs[i].key_table_name;
				if (!list.Contains(tname)) {
					list.Add(tname);
				}
			}

			return list.ToArray();
		}

		/// <summary>
		/// Returns a set of unique groups that are constrained to be unique 
		/// for the given table in this transaction.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableName"></param>
		/// <remarks>
		/// For example, if columns ('name') and ('number', 'document_rev') 
		/// are defined as unique, this will return an array of two groups 
		/// that represent unique columns in the given table.
		/// </remarks>
		/// <returns></returns>
		public static ColumnGroup[] QueryTableUniqueGroups(SimpleTransaction transaction, TableName tableName) {
			ITableDataSource t = transaction.GetTableDataSource(TableDataConglomerate.UniqueInfoTable);
			ITableDataSource t2 = transaction.GetTableDataSource(TableDataConglomerate.UniqueColsTable);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			ColumnGroup[] groups;
			try {
				// Returns the list indexes where column 3 = table name
				//                            and column 2 = schema name
				IList<int> data = dt.SelectEqual(3, tableName.Name,
				                                    2, tableName.Schema);
				groups = new ColumnGroup[data.Count];

				for (int i = 0; i < data.Count; ++i) {
					TObject id = dt.Get(0, data[i]);

					// Select all records with equal id
					IList<int> cols = dtcols.SelectEqual(0, id);

					// Put into a group.
					ColumnGroup group = new ColumnGroup();
					// constraint name
					group.name = dt.Get(1, data[i]).Object.ToString();
					group.columns = ToColumns(dtcols, cols);   // the list of columns
					group.deferred = (ConstraintDeferrability)((BigNumber)dt.Get(4, data[i]).Object).ToInt16();
					groups[i] = group;
				}
			} finally {
				dt.Dispose();
				dtcols.Dispose();
			}

			return groups;
		}

		/// <summary>
		/// Returns a set of primary key groups that are constrained to be unique
		/// for the given table in this transaction (there can be only 1 primary
		/// key defined for a table).
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableName"></param>
		/// <returns>
		/// Returns null if there is no primary key defined for the table.
		/// </returns>
		public static ColumnGroup QueryTablePrimaryKeyGroup(SimpleTransaction transaction, TableName tableName) {
			ITableDataSource t = transaction.GetTableDataSource(TableDataConglomerate.PrimaryInfoTable);
			ITableDataSource t2 = transaction.GetTableDataSource(TableDataConglomerate.PrimaryColsTable);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			try {
				// Returns the list indexes where column 3 = table name
				//                            and column 2 = schema name
				IList<int> data = dt.SelectEqual(3, tableName.Name,
				                                    2, tableName.Schema);

				if (data.Count > 1) {
					throw new ApplicationException("Assertion failed: multiple primary key for: " + tableName);
				} else if (data.Count == 1) {
					int rowIndex = data[0];
					// The id
					TObject id = dt.Get(0, rowIndex);
					// All columns with this id
					IList<int> ivec = dtcols.SelectEqual(0, id);
					// Make it in to a columns object
					ColumnGroup group = new ColumnGroup();
					group.name = dt.Get(1, rowIndex).Object.ToString();
					group.columns = ToColumns(dtcols, ivec);
					group.deferred = (ConstraintDeferrability)((BigNumber)dt.Get(4, rowIndex).Object).ToInt16();
					return group;
				} else {
					return null;
				}
			} finally {
				dt.Dispose();
				dtcols.Dispose();
			}
		}

		/// <summary>
		/// Returns a set of check expressions that are constrained over all 
		/// new columns added to the given table in this transaction.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableName"></param>
		/// <remarks>
		/// For example, we may want a column called 'serial_number' to be 
		/// constrained as CHECK serial_number LIKE '___-________-___'.
		/// </remarks>
		/// <returns></returns>
		public static CheckExpression[] QueryTableCheckExpressions(SimpleTransaction transaction, TableName tableName) {
			ITableDataSource t = transaction.GetTableDataSource(TableDataConglomerate.CheckInfoTable);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table

			CheckExpression[] checks;
			try {
				// Returns the list indexes where column 3 = table name
				//                            and column 2 = schema name
				IList<int> data = dt.SelectEqual(3, tableName.Name,
				                                    2, tableName.Schema);
				checks = new CheckExpression[data.Count];

				for (int i = 0; i < checks.Length; ++i) {
					int row_index = data[i];

					CheckExpression check = new CheckExpression();
					check.name = dt.Get(1, row_index).Object.ToString();
					check.deferred = (ConstraintDeferrability)((BigNumber)dt.Get(5, row_index).Object).ToInt16();
					// Is the deserialized version available?
					if (t.TableInfo.ColumnCount > 6) {
						ByteLongObject sexp = (ByteLongObject)dt.Get(6, row_index).Object;
						if (sexp != null) {
							try {
								// Deserialize the expression
								check.expression = (Expression)ObjectTranslator.Deserialize(sexp);
							} catch (Exception e) {
								// We weren't able to deserialize the expression so report the
								// error to the log
								transaction.Debug.Write(DebugLevel.Warning, typeof(Transaction),
											"Unable to deserialize the check expression.  " +
											"The error is: " + e.Message);
								transaction.Debug.Write(DebugLevel.Warning, typeof(Transaction),
											"Parsing the check expression instead.");

								check.expression = null;
							}
						}
					}
					// Otherwise we need to parse it from the string
					if (check.expression == null) {
						Expression exp = Expression.Parse(
											   dt.Get(4, row_index).Object.ToString());
						check.expression = exp;
					}
					checks[i] = check;
				}

			} finally {
				dt.Dispose();
			}

			return checks;
		}

		/// <summary>
		/// Returns an array of column references in the given table that 
		/// represent foreign key references.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableName"></param>
		/// <remarks>
		/// This method is used to check that a foreign key reference actually 
		/// points to a valid record in the referenced table as expected.
		/// </remarks>
		/// <returns></returns>
		/// <example>
		/// For example, say a foreign reference has been set up in the given 
		/// table as follows:
		/// <code>
		/// FOREIGN KEY (customer_id) REFERENCES Customer (id)
		/// </code>
		/// This method will return the column group reference
		/// Order(customer_id) -> Customer(id).
		/// </example>
		public static ColumnGroupReference[] QueryTableForeignKeyReferences(SimpleTransaction transaction, TableName tableName) {
			ITableDataSource t = transaction.GetTableDataSource(TableDataConglomerate.ForeignInfoTable);
			ITableDataSource t2 = transaction.GetTableDataSource(TableDataConglomerate.ForeignColsTable);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			ColumnGroupReference[] groups;
			try {
				// Returns the list indexes where column 3 = table name
				//                            and column 2 = schema name
				IList<int> data = dt.SelectEqual(3, tableName.Name,
				                                    2, tableName.Schema);
				groups = new ColumnGroupReference[data.Count];

				for (int i = 0; i < data.Count; ++i) {
					int rowIndex = data[i];

					// The foreign key id
					TObject id = dt.Get(0, rowIndex);

					// The referenced table
					TableName ref_table_name = new TableName(
							   dt.Get(4, rowIndex).Object.ToString(),
							   dt.Get(5, rowIndex).Object.ToString());

					// Select all records with equal id
					IList<int> cols = dtcols.SelectEqual(0, id);

					// Put into a group.
					ColumnGroupReference group = new ColumnGroupReference();
					// constraint name
					group.name = dt.Get(1, rowIndex).Object.ToString();
					group.key_table_name = tableName;
					group.ref_table_name = ref_table_name;
					group.update_rule = (ConstraintAction)dt.Get(6, rowIndex).ToBigNumber().ToInt32();
					group.delete_rule = (ConstraintAction)dt.Get(7, rowIndex).ToBigNumber().ToInt32();
					group.deferred = (ConstraintDeferrability)((BigNumber)dt.Get(8, rowIndex).Object).ToInt16();

					int colsSize = cols.Count;
					string[] keyCols = new string[colsSize];
					string[] refCols = new string[colsSize];
					for (int n = 0; n < colsSize; ++n) {
						for (int p = 0; p < colsSize; ++p) {
							int cols_index = cols[p];
							if (((BigNumber)dtcols.Get(3, cols_index).Object).ToInt32() == n) {
								keyCols[n] = dtcols.Get(1, cols_index).Object.ToString();
								refCols[n] = dtcols.Get(2, cols_index).Object.ToString();
								break;
							}
						}
					}
					group.key_columns = keyCols;
					group.ref_columns = refCols;

					groups[i] = group;
				}
			} finally {
				dt.Dispose();
				dtcols.Dispose();
			}

			return groups;
		}

		/// <summary>
		/// Returns an array of column references in the given table that represent
		/// foreign key references that reference columns in the given table.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="refTableName"></param>
		/// <remarks>
		/// This is a reverse mapping of the <see cref="QueryTableForeignKeyReferences"/>
		/// method.
		///	<para>
		///	This method is used to check that a reference isn't broken when we 
		///	remove a record (for example, removing a Customer that has references 
		///	to it will break integrity).
		///	</para>
		/// </remarks>
		/// <example>
		/// Say a foreign reference has been set up in any table as follows:
		/// <code>
		/// [ In table Order ]
		///		FOREIGN KEY (customer_id) REFERENCE Customer (id)
		/// </code>
		/// And the table name we are querying is <i>Customer</i> then this 
		/// method will return the column group reference
		/// <code>
		///		Order(customer_id) -> Customer(id).
		///	</code>
		/// </example>
		/// <returns></returns>
		public static ColumnGroupReference[] QueryTableImportedForeignKeyReferences(SimpleTransaction transaction, TableName refTableName) {
			ITableDataSource t = transaction.GetTableDataSource(TableDataConglomerate.ForeignInfoTable);
			ITableDataSource t2 = transaction.GetTableDataSource(TableDataConglomerate.ForeignColsTable);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			ColumnGroupReference[] groups;
			try {
				// Returns the list indexes where column 5 = ref table name
				//                            and column 4 = ref schema name
				IList<int> data = dt.SelectEqual(5, refTableName.Name,
				                                    4, refTableName.Schema);
				groups = new ColumnGroupReference[data.Count];

				for (int i = 0; i < data.Count; ++i) {
					int rowIndex = data[i];

					// The foreign key id
					TObject id = dt.Get(0, rowIndex);

					// The referencee table
					TableName table_name = new TableName(
						  dt.Get(2, rowIndex).Object.ToString(),
						  dt.Get(3, rowIndex).Object.ToString());

					// Select all records with equal id
					IList<int> cols = dtcols.SelectEqual(0, id);

					// Put into a group.
					ColumnGroupReference group = new ColumnGroupReference();
					// constraint name
					group.name = dt.Get(1, rowIndex).Object.ToString();
					group.key_table_name = table_name;
					group.ref_table_name = refTableName;
					group.update_rule = (ConstraintAction)dt.Get(6, rowIndex).ToBigNumber().ToInt32();
					group.delete_rule = (ConstraintAction)dt.Get(7, rowIndex).ToBigNumber().ToInt32();
					group.deferred = (ConstraintDeferrability)((BigNumber)dt.Get(8, rowIndex).Object).ToInt16();

					int colsSize = cols.Count;
					string[] keyCols = new string[colsSize];
					string[] refCols = new string[colsSize];
					for (int n = 0; n < colsSize; ++n) {
						for (int p = 0; p < colsSize; ++p) {
							int cols_index = cols[p];
							if (((BigNumber)dtcols.Get(3, cols_index).Object).ToInt32() == n) {
								keyCols[n] = dtcols.Get(1, cols_index).Object.ToString();
								refCols[n] = dtcols.Get(2, cols_index).Object.ToString();
								break;
							}
						}
					}
					group.key_columns = keyCols;
					group.ref_columns = refCols;

					groups[i] = group;
				}
			} finally {
				dt.Dispose();
				dtcols.Dispose();
			}

			return groups;
		}

	}
}