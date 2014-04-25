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
using System.Collections.Generic;
using System.IO;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Transactions {
	public static class CommittableTransactionExtensions {
		/// <summary>
		/// Create a new schema in this transaction.
		/// </summary>
		/// <param name="name">The name of the schema to create.</param>
		/// <param name="type">The type to assign to the schema.</param>
		/// <remarks>
		/// When the transaction is committed the schema will become globally 
		/// accessable.
		/// <para>
		/// Any security checks must be performed before this method is called.
		/// </para>
		/// <para>
		/// <b>Note</b>: We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		/// <exception cref="StatementException">
		/// If a schema with the same <paramref name="name"/> already exists.
		/// </exception>
		public static void CreateSchema(this ICommitableTransaction transaction, string name, string type) {
			TableName tableName = SystemSchema.SchemaInfoTable;
			IMutableTableDataSource t = transaction.GetMutableTable(tableName);
			SimpleTableQuery dt = new SimpleTableQuery(t);

			try {
				// Select entries where;
				//     schema_info.name = name
				if (dt.Exists(1, name))
					throw new StatementException("Schema already exists: " + name);

				// Add the entry to the schema info table.
				DataRow rd = new DataRow(t);
				BigNumber uniqueId = transaction.NextUniqueId(tableName);
				rd.SetValue(0, uniqueId);
				rd.SetValue(1, name);
				rd.SetValue(2, type);
				// Third (other) column is left as null
				t.AddRow(rd);
			} finally {
				dt.Dispose();
			}
		}

		/// <summary>
		/// Drops a new schema in this transaction.
		/// </summary>
		/// <param name="name">The name of the schema to drop.</param>
		/// <remarks>
		/// When the transaction is committed the schema will become globally 
		/// accessable.
		/// <para>
		/// Note that any security checks must be performed before this method 
		/// is called.
		/// </para>
		/// <para>
		/// <b>Note</b> We must guarentee that the transaction be in exclusive 
		/// mode before this method is called.
		/// </para>
		/// </remarks>
		public static void DropSchema(this ICommitableTransaction transaction, string name) {
			TableName tableName = SystemSchema.SchemaInfoTable;
			IMutableTableDataSource t = transaction.GetMutableTable(tableName);
			SimpleTableQuery dt = new SimpleTableQuery(t);

			// Drop a single entry from dt where column 1 = name
			try {
				if (!dt.Delete(1, name))
					throw new StatementException("Schema doesn't exists: " + name);
			} finally {
				dt.Dispose();
			}
		}

		/// <summary>
		/// Creates a new table within this transaction with the given sector 
		/// size.
		/// </summary>
		/// <param name="tableInfo"></param>
		/// <remarks>
		/// This should only be called under an exclusive lock on the connection.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table already exists.
		/// </exception>
		public static void CreateTable(this ICommitableTransaction transaction, DataTableInfo tableInfo) {
			// data sector size defaults to 251
			// index sector size defaults to 1024
			transaction.CreateTable(tableInfo, 251, 1024);
		}

		/// <summary>
		/// Alters the table with the given name within this transaction to the
		/// specified table definition.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableName"></param>
		/// <param name="tableInfo"></param>
		/// <remarks>
		/// This should only be called under an exclusive lock on the connection.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If the table does not exist.
		/// </exception>
		public static void AlterTable(this ICommitableTransaction transaction, TableName tableName, DataTableInfo tableInfo) {
			// Make sure we remember the current sector size of the altered table so
			// we can create the new table with the original size.
			try {
				// HACK: We use index sector size of 2043 for all altered tables
				transaction.AlterTable(tableName, tableInfo, -1, 2043);

			} catch (IOException e) {
				throw new Exception("IO Error: " + e.Message);
			}
		}

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

		public static void AddConstraint(this ICommitableTransaction transaction, DataConstraintInfo constraint) {
			if (constraint.Type == ConstraintType.Check) {
				transaction.AddCheckConstraint(constraint);
			} else if (constraint.Type == ConstraintType.ForeignKey) {
				transaction.AddForeignKeyConstraint(constraint);
			} else if (constraint.Type == ConstraintType.PrimaryKey) {
				transaction.AddPrimaryKeyConstraint(constraint);
			} else if (constraint.Type == ConstraintType.Unique) {
				transaction.AddPrimaryKeyConstraint(constraint);
			} else {
				throw new ArgumentException("Constraint type not supported.");
			}
		}

		/// <summary>
		/// Adds a unique constraint to the database which becomes perminant 
		/// when the transaction is committed.
		/// </summary>
		/// <param name="constraint">The unique constraint to add.</param>
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
		public static void AddUniqueConstraint(this ICommitableTransaction transaction, DataConstraintInfo constraint) {
			if (constraint.Type != ConstraintType.Unique)
				throw new ArgumentException("The constraint given is not a UNIQUE", "constraint");

			transaction.AddUniqueConstraint(constraint.TableName, constraint.Columns, constraint.Deferred, constraint.Name);
		}

		/// <summary>
		/// Adds a unique constraint to the database which becomes perminant 
		/// when the transaction is committed.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableName"></param>
		/// <param name="columns"></param>
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
		public static void AddUniqueConstraint(this ICommitableTransaction transaction, TableName tableName, string[] columns, ConstraintDeferrability deferred, string constraintName) {
			TableName tn1 = SystemSchema.UniqueInfoTable;
			TableName tn2 = SystemSchema.UniqueColsTable;
			IMutableTableDataSource t = transaction.GetMutableTable(tn1);
			IMutableTableDataSource tcols = transaction.GetMutableTable(tn2);

			try {

				// Insert a value into UniqueInfoTable
				DataRow row = new DataRow(t);
				BigNumber uniqueId = transaction.NextUniqueId(tn1);
				constraintName = MakeUniqueConstraintName(constraintName, uniqueId);
				row.SetValue(0, uniqueId);
				row.SetValue(1, constraintName);
				row.SetValue(2, tableName.Schema);
				row.SetValue(3, tableName.Name);
				row.SetValue(4, (BigNumber)((short)deferred));
				t.AddRow(row);

				// Insert the columns
				for (int i = 0; i < columns.Length; ++i) {
					row = new DataRow(tcols);
					row.SetValue(0, uniqueId);            // unique id
					row.SetValue(1, columns[i]);              // column name
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
		/// <param name="constraint">The foreign key constraint object to add.</param>
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
		public static void AddForeignKeyConstraint(this ICommitableTransaction transaction, DataConstraintInfo constraint) {
			if (constraint.Type != ConstraintType.ForeignKey)
				throw new ArgumentException("Constraint given is not a FOREIGN KEY", "constraint");

			transaction.AddForeignKeyConstraint(constraint.TableName, constraint.Columns, constraint.ReferencedTableName,
			                        constraint.ReferencedColumns, constraint.DeleteRule, constraint.UpdateRule,
			                        constraint.Deferred, constraint.Name);
		}

		/// <summary>
		/// Adds a foreign key constraint to the database which becomes perminent
		/// when the transaction is committed.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="table">The key table to link from.</param>
		/// <param name="columns">The key columns to link from</param>
		/// <param name="refTable">The referenced table to link to.</param>
		/// <param name="refColumns">The refenced columns to link to.</param>
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
		public static void AddForeignKeyConstraint(this ICommitableTransaction transaction, TableName table, string[] columns, 
			TableName refTable, string[] refColumns, 
			ConstraintAction deleteRule, ConstraintAction updateRule, ConstraintDeferrability deferred, String constraintName) {
			TableName tn1 = SystemSchema.ForeignInfoTable;
			TableName tn2 = SystemSchema.ForeignColsTable;
			IMutableTableDataSource t = transaction.GetMutableTable(tn1);
			IMutableTableDataSource tcols = transaction.GetMutableTable(tn2);

			try {
				// If 'ref_columns' empty then set to primary key for referenced table,
				// ISSUE: What if primary key changes after the fact?
				if (refColumns.Length == 0) {
					DataConstraintInfo set = transaction.QueryTablePrimaryKey(refTable);
					if (set == null)
						throw new StatementException("No primary key defined for referenced table '" + refTable + "'");

					refColumns = set.Columns;
				}

				if (columns.Length != refColumns.Length) {
					throw new StatementException("Foreign key reference '" + table +
					  "' -> '" + refTable + "' does not have an equal number of " +
					  "column terms.");
				}

				// If delete or update rule is 'SET NULL' then check the foreign key
				// columns are not constrained as 'NOT NULL'
				if (deleteRule == ConstraintAction.SetNull ||
					updateRule == ConstraintAction.SetNull) {
					DataTableInfo tableInfo = transaction.GetTableInfo(table);
					for (int i = 0; i < columns.Length; ++i) {
						DataColumnInfo columnInfo = tableInfo[tableInfo.FindColumnName(columns[i])];
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
				BigNumber uniqueId = transaction.NextUniqueId(tn1);
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
				for (int i = 0; i < columns.Length; ++i) {
					row = new DataRow(tcols);
					row.SetValue(0, uniqueId);            // unique id
					row.SetValue(1, columns[i]);              // column name
					row.SetValue(2, refColumns[i]);          // ref column name
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
		/// <param name="transaction"></param>
		/// <param name="constraint">The primary key constraint to add.</param>
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
		public static void AddPrimaryKeyConstraint(this ICommitableTransaction transaction, DataConstraintInfo constraint) {
			if (constraint.Type != ConstraintType.PrimaryKey)
				throw new ArgumentException("The constraint given is not a PRIMARY KEY.", "constraint");

			transaction.AddPrimaryKeyConstraint(constraint.TableName, constraint.Columns, constraint.Deferred, constraint.Name);
		}

		/// <summary>
		/// Adds a primary key constraint that becomes perminent when the 
		/// transaction is committed.
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="columns"></param>
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
		public static void AddPrimaryKeyConstraint(this ICommitableTransaction transaction, TableName tableName, string[] columns, ConstraintDeferrability deferred, string constraintName) {
			TableName tn1 = SystemSchema.PrimaryInfoTable;
			TableName tn2 = SystemSchema.PrimaryColsTable;
			IMutableTableDataSource t = transaction.GetMutableTable(tn1);
			IMutableTableDataSource tcols = transaction.GetMutableTable(tn2);

			try {
				// Insert a value into PrimaryInfoTable
				DataRow row = new DataRow(t);
				BigNumber uniqueId = transaction.NextUniqueId(tn1);
				constraintName = MakeUniqueConstraintName(constraintName, uniqueId);
				row.SetValue(0, uniqueId);
				row.SetValue(1, constraintName);
				row.SetValue(2, tableName.Schema);
				row.SetValue(3, tableName.Name);
				row.SetValue(4, (BigNumber)((short)deferred));
				t.AddRow(row);

				// Insert the columns
				for (int i = 0; i < columns.Length; ++i) {
					row = new DataRow(tcols);
					row.SetValue(0, uniqueId);            // unique id
					row.SetValue(1, columns[i]);              // column name
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
		/// <param name="transaction"></param>
		/// <param name="constraint">The check constraint to add.</param>
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
		public static void AddCheckConstraint(this ICommitableTransaction transaction, DataConstraintInfo constraint) {
			if (constraint.Type != ConstraintType.Check)
				throw new ArgumentException("The constraint given is not a CHECK.", "constraint");

			transaction.AddCheckConstraint(constraint.TableName, constraint.CheckExpression, constraint.Deferred, constraint.Name);
		}

		/// <summary>
		/// Adds a check expression that becomes perminent when the transaction
		/// is committed.
		/// </summary>
		/// <param name="transaction"></param>
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
		public static void AddCheckConstraint(this ICommitableTransaction transaction, TableName tableName, Expression expression, ConstraintDeferrability deferred, string constraintName) {
			TableName tn = SystemSchema.CheckInfoTable;
			IMutableTableDataSource t = transaction.GetMutableTable(tn);
			int colCount = t.TableInfo.ColumnCount;

			try {
				// Insert check constraint data.
				BigNumber uniqueId = transaction.NextUniqueId(tn);
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
					throw new StatementException("Check constraint name '" + constraintName + "' is already being used.");
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
		public static void DropAllConstraintsForTable(this ICommitableTransaction transaction, TableName tableName) {
			DataConstraintInfo primary = transaction.QueryTablePrimaryKey(tableName);
			DataConstraintInfo[] uniques = transaction.QueryTableUniques(tableName);
			DataConstraintInfo[] expressions = transaction.QueryTableCheckExpressions(tableName);
			DataConstraintInfo[] refs = transaction.QueryTableForeignKeys(tableName);

			if (primary != null)
				transaction.DropPrimaryKeyConstraintForTable(tableName, primary.Name);
			foreach (DataConstraintInfo unique in uniques) {
				transaction.DropUniqueConstraintForTable(tableName, unique.Name);
			}
			foreach (DataConstraintInfo expression in expressions) {
				transaction.DropCheckConstraintForTable(tableName, expression.Name);
			}
			foreach (DataConstraintInfo reference in refs) {
				transaction.DropForeignKeyReferenceConstraintForTable(tableName, reference.Name);
			}
		}

		/// <summary>
		/// Drops the named constraint from the transaction.
		/// </summary>
		/// <param name="transaction"></param>
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
		public static int DropNamedConstraint(this ICommitableTransaction transaction, TableName tableName, string constraintName) {
			int dropCount = 0;
			if (transaction.DropPrimaryKeyConstraintForTable(tableName, constraintName)) {
				++dropCount;
			}
			if (transaction.DropUniqueConstraintForTable(tableName, constraintName)) {
				++dropCount;
			}
			if (transaction.DropCheckConstraintForTable(tableName, constraintName)) {
				++dropCount;
			}
			if (transaction.DropForeignKeyReferenceConstraintForTable(tableName, constraintName)) {
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
		public static bool DropPrimaryKeyConstraintForTable(this ICommitableTransaction transaction, TableName tableName, string constraintName) {
			IMutableTableDataSource t = transaction.GetMutableTable(SystemSchema.PrimaryInfoTable);
			IMutableTableDataSource t2 = transaction.GetMutableTable(SystemSchema.PrimaryColsTable);
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
		public static bool DropUniqueConstraintForTable(this ICommitableTransaction transaction, TableName table, string constraintName) {
			IMutableTableDataSource t = transaction.GetMutableTable(SystemSchema.UniqueInfoTable);
			IMutableTableDataSource t2 = transaction.GetMutableTable(SystemSchema.UniqueColsTable);
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
		/// <param name="transaction"></param>
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
		public static bool DropCheckConstraintForTable(this ICommitableTransaction transaction, TableName table, string constraintName) {
			IMutableTableDataSource t = transaction.GetMutableTable(SystemSchema.CheckInfoTable);
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
		/// <param name="transaction"></param>
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
		public static bool DropForeignKeyReferenceConstraintForTable(this ICommitableTransaction transaction, TableName table, string constraintName) {
			IMutableTableDataSource t = transaction.GetMutableTable(SystemSchema.ForeignInfoTable);
			IMutableTableDataSource t2 = transaction.GetMutableTable(SystemSchema.ForeignColsTable);
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
		/// Sets a persistent variable of the database that becomes a committed
		/// change once this transaction is committed.
		/// </summary>
		/// <param name="variable"></param>
		/// <param name="value"></param>
		/// <remarks>
		/// The variable can later be retrieved with a call to the 
		/// <see cref="GetPersistantVariable"/> method.  A persistant var is created 
		/// if it doesn't exist in the DatabaseVars table otherwise it is 
		/// overwritten.
		/// </remarks>
		public static void SetPersistentVariable(this ICommitableTransaction transaction, string variable, string value) {
			TableName tableName = SystemSchema.PersistentVarTable;
			ITableDataSource t = transaction.GetMutableTable(tableName);
			var dt = new SimpleTableQuery(t);
			dt.SetVariable(0, new Object[] { variable, value });
			dt.Dispose();
		}
	}
}