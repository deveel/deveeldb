// 
//  Copyright 2010-2015 Deveel
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
//

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Deveel.Data;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Tables;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Transactions {
	public static class TransactionConstraintExtensions {
		public static void AddPrimaryKey(this ITransaction transaction, ObjectName tableName, string[] columns,
			string constraintName) {
			AddPrimaryKey(transaction, tableName, columns, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddPrimaryKey(this ITransaction transaction, ObjectName tableName, string[] columns,
			ConstraintDeferrability deferred, string constraintName) {
			var t = transaction.GetMutableTable(SystemSchema.PrimaryKeyInfoTableName);
			var tcols = transaction.GetMutableTable(SystemSchema.PrimaryKeyColumnsTableName);

			try {
				// Insert a value into PrimaryInfoTable
				var row = t.NewRow();
				var uniqueId = transaction.NextTableId(SystemSchema.PrimaryKeyInfoTableName);
				constraintName = MakeUniqueConstraintName(constraintName, uniqueId);
				row.SetValue(0, uniqueId);
				row.SetValue(1, constraintName);
				row.SetValue(2, tableName.Parent.Name);
				row.SetValue(3, tableName.Name);
				row.SetValue(4, (short) deferred);
				t.AddRow(row);

				// Insert the columns
				for (int i = 0; i < columns.Length; ++i) {
					row = tcols.NewRow();
					row.SetValue(0, uniqueId); // unique id
					row.SetValue(1, columns[i]); // column name
					row.SetValue(2, i); // Sequence number
					tcols.AddRow(row);
				}

			} catch (ConstraintViolationException e) {
				// Constraint violation when inserting the data.  Check the type and
				// wrap around an appropriate error message.
				if (e.ErrorCode == SqlModelErrorCodes.UniqueViolation) {
					// This means we gave a constraint name that's already being used
					// for a primary key.
					throw new Exception(String.Format("Primary key constraint name '{0}' is already being used.", constraintName));
				}

				throw;
			}
		}

		public static void AddForeignKey(this ITransaction transaction, ObjectName table, string[] columns,
			ObjectName refTable, string[] refColumns,
			ForeignKeyAction deleteRule, ForeignKeyAction updateRule, String constraintName) {
			AddForeignKey(transaction, table, columns, refTable, refColumns, deleteRule, updateRule,
				ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddForeignKey(this ITransaction transaction, ObjectName table, string[] columns,
			ObjectName refTable, string[] refColumns, String constraintName) {
			AddForeignKey(transaction, table, columns, refTable, refColumns, ConstraintDeferrability.InitiallyImmediate,
				constraintName);
		}

		public static void AddForeignKey(this ITransaction transaction, ObjectName table, string[] columns,
			ObjectName refTable, string[] refColumns, ConstraintDeferrability deferred, String constraintName) {
			AddForeignKey(transaction, table, columns, refTable, refColumns, ForeignKeyAction.NoAction, ForeignKeyAction.NoAction,
				deferred, constraintName);
		}

		public static void AddForeignKey(this ITransaction transaction, ObjectName table, string[] columns,
			ObjectName refTable, string[] refColumns,
			ForeignKeyAction deleteRule, ForeignKeyAction updateRule, ConstraintDeferrability deferred, String constraintName) {
			var t = transaction.GetMutableTable(SystemSchema.ForeignKeyInfoTableName);
			var tcols = transaction.GetMutableTable(SystemSchema.ForeignKeyColumnsTableName);

			try {
				// If 'ref_columns' empty then set to primary key for referenced table,
				// ISSUE: What if primary key changes after the fact?
				if (refColumns.Length == 0) {
					var set = transaction.QueryTablePrimaryKey(refTable);
					if (set == null)
						throw new Exception(String.Format("No primary key defined for referenced table '{0}'", refTable));

					refColumns = set.ColumnNames;
				}

				if (columns.Length != refColumns.Length) {
					throw new Exception(String.Format("Foreign key reference '{0}' -> '{1}' does not have an equal number of " +
					                                  "column terms.", table, refTable));
				}

				// If delete or update rule is 'SET NULL' then check the foreign key
				// columns are not constrained as 'NOT NULL'
				if (deleteRule == ForeignKeyAction.SetNull ||
				    updateRule == ForeignKeyAction.SetNull) {
					var tableInfo = transaction.GetTableInfo(table);
					for (int i = 0; i < columns.Length; ++i) {
						var columnInfo = tableInfo[tableInfo.IndexOfColumn(columns[i])];
						if (columnInfo.IsNotNull) {
							throw new Exception(String.Format("Foreign key reference '{0}' -> '{1}' update or delete triggered " +
							                                  "action is SET NULL for columns that are constrained as " +
							                                  "NOT NULL.", table, refTable));
						}
					}
				}

				// Insert a value into ForeignInfoTable
				var row = t.NewRow();
				var uniqueId = transaction.NextTableId(SystemSchema.ForeignKeyInfoTableName);
				constraintName = MakeUniqueConstraintName(constraintName, uniqueId);
				row.SetValue(0, uniqueId);
				row.SetValue(1, constraintName);
				row.SetValue(2, table.Parent.Name);
				row.SetValue(3, table.Name);
				row.SetValue(4, refTable.Parent.Name);
				row.SetValue(5, refTable.Name);
				row.SetValue(6, ((int) updateRule));
				row.SetValue(7, ((int) deleteRule));
				row.SetValue(8, ((short) deferred));
				t.AddRow(row);

				// Insert the columns
				for (int i = 0; i < columns.Length; ++i) {
					row = tcols.NewRow();
					row.SetValue(0, uniqueId); // unique id
					row.SetValue(1, columns[i]); // column name
					row.SetValue(2, refColumns[i]); // ref column name
					row.SetValue(3, i); // sequence number
					tcols.AddRow(row);
				}

			} catch (ConstraintViolationException e) {
				// Constraint violation when inserting the data.  Check the type and
				// wrap around an appropriate error message.
				if (e.ErrorCode == SqlModelErrorCodes.UniqueViolation)

					// This means we gave a constraint name that's already being used
					// for a primary key.
					throw new Exception(String.Format("Foreign key constraint name '{0}' is already being used.", constraintName));

				throw;
			}
		}

		public static void AddUniqueKey(this ITransaction transaction, ObjectName tableName, string[] columns,
			string constraintName) {
			AddUniqueKey(transaction, tableName, columns, ConstraintDeferrability.InitiallyImmediate, constraintName);
		}

		public static void AddUniqueKey(this ITransaction transaction, ObjectName tableName, string[] columns,
			ConstraintDeferrability deferred, string constraintName) {
			var t = transaction.GetMutableTable(SystemSchema.UniqueKeyInfoTableName);
			var tcols = transaction.GetMutableTable(SystemSchema.UniqueKeyColumnsTableName);

			try {
				// Insert a value into UniqueInfoTable
				var row = t.NewRow();
				var uniqueId = transaction.NextTableId(SystemSchema.UniqueKeyInfoTableName);
				constraintName = MakeUniqueConstraintName(constraintName, uniqueId);
				row.SetValue(0, uniqueId);
				row.SetValue(1, constraintName);
				row.SetValue(2, tableName.Parent.Name);
				row.SetValue(3, tableName.Name);
				row.SetValue(4, (short) deferred);
				t.AddRow(row);

				// Insert the columns
				for (int i = 0; i < columns.Length; ++i) {
					row = tcols.NewRow();
					row.SetValue(0, uniqueId); // unique id
					row.SetValue(1, columns[i]); // column name
					row.SetValue(2, i); // sequence number
					tcols.AddRow(row);
				}

			} catch (ConstraintViolationException e) {
				// Constraint violation when inserting the data.  Check the type and
				// wrap around an appropriate error message.
				if (e.ErrorCode == SqlModelErrorCodes.UniqueViolation)

					// This means we gave a constraint name that's already being used
					// for a primary key.
					throw new Exception(String.Format("Unique constraint name '{0}' is already being used.", constraintName));

				throw;
			}
		}

		public static void AddCheck(this ITransaction transaction, ObjectName tableName, SqlExpression expression,
			ConstraintDeferrability deferrability, string constraintName) {
			var tn = SystemSchema.CheckInfoTableName;
			var t = transaction.GetMutableTable(tn);
			int colCount = t.TableInfo.ColumnCount;

			try {
				byte[] binExp;
				using (var stream = new MemoryStream()) {
					using (var writer = new BinaryWriter(stream, Encoding.Unicode)) {
						SqlExpression.Serialize(expression, writer);
						writer.Flush();

						binExp = stream.ToArray();
					}
				}

				// Insert check constraint data.
				var uniqueId = transaction.NextTableId(tn);
				constraintName = MakeUniqueConstraintName(constraintName, uniqueId);
				var rd = t.NewRow();
				rd.SetValue(0, uniqueId);
				rd.SetValue(1, constraintName);
				rd.SetValue(2, tableName.ParentName);
				rd.SetValue(3, tableName.Name);
				rd.SetValue(4, expression.ToString());
				rd.SetValue(5, (short) deferrability);
				if (colCount > 6) {
					rd.SetValue(6, Field.Binary(new SqlBinary(binExp)));
				}

				t.AddRow(rd);

			} catch (ConstraintViolationException e) {
				// Constraint violation when inserting the data.  Check the type and
				// wrap around an appropriate error message.
				if (e.ErrorCode == SqlModelErrorCodes.UniqueViolation) {
					// This means we gave a constraint name that's already being used.
					throw new InvalidOperationException("Check constraint name '" + constraintName + "' is already being used.");
				}
				throw;
			}
		}

		private static string MakeUniqueConstraintName(string constraintName, SqlNumber uniqueId) {
			return String.IsNullOrEmpty(constraintName) ? ("_ANONYMOUS_CONSTRAINT_" + uniqueId) : constraintName;
		}

		private static String[] ToColumns(ITable table, IEnumerable<int> cols) {
			var colList = cols.ToList();
			int size = colList.Count;
			var list = new String[size];

			// for each n of the output list
			for (int n = 0; n < size; ++n) {
				// for each i of the input list
				for (int i = 0; i < size; ++i) {
					int rowIndex = colList[i];
					int seqNo = ((SqlNumber) table.GetValue(rowIndex, 2).Value).ToInt32();
					if (seqNo == n) {
						list[n] = table.GetValue(rowIndex, 1).Value.ToString();
						break;
					}
				}
			}

			return list;
		}

		private static bool IsUniqueColumns(ITable table, int rindex, string[] cols, bool nullsAllowed) {
			var tableInfo = table.TableInfo;

			// 'identical_rows' keeps a tally of the rows that match our added cell.
			IList<int> identicalRows = null;

			// Resolve the list of column names to column indexes
			var colIndexes = tableInfo.IndexOfColumns(cols).ToList();

			// If the value being tested for uniqueness contains NULL, we return true
			// if nulls are allowed.
			if (colIndexes.Select(x => table.GetValue(rindex, x)).Any(x => x.IsNull))
				return nullsAllowed;

			foreach (var colIndex in colIndexes) {
				var value = table.GetValue(rindex, colIndex);

				// We are assured of uniqueness if 'identicalRows != null &&
				// identicalRows.Count == 0'  This is because 'identicalRows' keeps
				// a running tally of the rows in the table that contain unique columns
				// whose cells match the record being added.

				if (identicalRows == null || identicalRows.Count > 0) {
					// Ask SelectableScheme to return pointers to row(s) if there is
					// already a cell identical to this in the table.

					var index = table.GetIndex(colIndex);
					var list = index.SelectEqual(value).ToList();

					// If 'identical_rows' hasn't been set up yet then set it to 'ivec'
					// (the list of rows where there is a cell which is equal to the one
					//  being added)
					// If 'identical_rows' has been set up, then perform an
					// 'intersection' operation on the two lists (only keep the numbers
					// that are repeated in both lists).  Therefore we keep the rows
					// that match the row being added.

					if (identicalRows == null) {
						identicalRows = list;
					} else {
						list.Sort();
						int rowIndex = identicalRows.Count - 1;
						while (rowIndex >= 0) {
							int val = identicalRows[rowIndex];
							int foundIndex = list.BinarySearch(val);

							// If we _didn't_ find the index in the array
							if (foundIndex < 0 ||
							    list[foundIndex] != val) {
								identicalRows.RemoveAt(rowIndex);
							}
							--rowIndex;
						}
					}
				}
			}

			// If there is 1 (the row we added) then we are unique, otherwise we are
			// not.
			if (identicalRows != null) {
				int sz = identicalRows.Count;
				if (sz == 1)
					return true;
				if (sz > 1)
					return false;
				if (sz == 0)
					throw new InvalidOperationException("Assertion failed: We must be able to find the " +
					                                    "row we are testing uniqueness against!");
			}

			return true;
		}

		private static int RowCountOfReferenceTable(this ITransaction transaction, int rowIndex, ObjectName table1,
			string[] cols1, ObjectName table2, String[] cols2,
			bool checkSourceTableKey) {

			// Get the tables
			var t1 = transaction.GetTable(table1);
			var t2 = transaction.GetTable(table2);

			// The table defs
			var dti1 = t1.TableInfo;
			var dti2 = t2.TableInfo;

			// Resolve the list of column names to column indexes
			var col1Indexes = dti1.IndexOfColumns(cols1).ToArray();
			var col2Indexes = dti2.IndexOfColumns(cols2).ToArray();

			int keySize = col1Indexes.Length;

			// Get the data from table1
			var keyValue = new Field[keySize];
			int nullCount = 0;
			for (int n = 0; n < keySize; ++n) {
				keyValue[n] = t1.GetValue(rowIndex, col1Indexes[n]);
				if (keyValue[n].IsNull) {
					++nullCount;
				}
			}

			// If we are searching for null then return -1;
			if (nullCount > 0)
				return -1;

			// HACK: This is a hack.  The purpose is if the key exists in the source
			//   table we return 0 indicating to the delete check that there are no
			//   references and it's valid.  To the semantics of the method this is
			//   incorrect.
			if (checkSourceTableKey) {
				var keys = t1.FindKeys(col1Indexes, keyValue);
				if (keys.Any())
					return 0;
			}

			return t2.FindKeys(col2Indexes, keyValue).Count();
		}

		public static void CheckFieldConstraintViolations(this ITransaction transaction, ITable table, int[] rowIndices) {
			if (rowIndices == null || rowIndices.Length == 0)
				return;

			// Check for any bad cells - which are either cells that are 'null' in a
			// column declared as 'not null', or duplicated in a column declared as
			// unique.

			var tableInfo = table.TableInfo;

			// Check not-null columns are not null.  If they are null, throw an
			// error.  Additionally check that OBJECT columns are correctly
			// typed.

			// Check each field of the added rows
			int len = tableInfo.ColumnCount;
			for (int i = 0; i < len; ++i) {
				// Get the column definition and the cell being inserted,
				var columnInfo = tableInfo[i];

				// For each row added to this column
				for (int rn = 0; rn < rowIndices.Length; ++rn) {
					var value = table.GetValue(rowIndices[rn], i);

					// Check: Column defined as not null and cell being inserted is
					// not null.
					if (columnInfo.IsNotNull && value.IsNull) {
						throw new ConstraintViolationException(
							SqlModelErrorCodes.NullableViolation,
							"Attempt to set NULL value to column '" +
							tableInfo[i].ColumnName +
							"' which is declared as NOT NULL");
					}

					// Check: If column is an object, then deserialize and check the
					//        object is an instance of the class constraint,
					if (!value.IsNull &&
					    columnInfo.ColumnType.TypeCode == SqlTypeCode.Object) {
						throw new NotImplementedException(); // TODO:
					}
				}
			}
		}

		public static void CheckAddConstraintViolations(this ITransaction transaction, ITable table,
			ConstraintDeferrability deferred) {
			// Get all the rows in the table
			var rows = table.Select(x => x.RowId.RowNumber).ToArray();

			// Check the constraints of all the rows in the table.
			CheckAddConstraintViolations(transaction, table, rows, deferred);
		}

		public static void CheckAddConstraintViolations(this ITransaction transaction, ITable table, int[] rowIndices,
			ConstraintDeferrability deferred) {
			string curSchema = table.TableInfo.TableName.Parent.Name;
			using (var session = new SystemSession(transaction, curSchema)) {
				using (var queryContext = session.CreateQuery()) {

					// Quick exit case
					if (rowIndices == null || rowIndices.Length == 0)
						return;

					var tableInfo = table.TableInfo;
					var tableName = tableInfo.TableName;

					// ---- Constraint checking ----

					// Check any primary key constraint.
					var primaryKey = transaction.QueryTablePrimaryKey(tableName);
					if (primaryKey != null &&
					    (deferred == ConstraintDeferrability.InitiallyDeferred ||
					     primaryKey.Deferred == ConstraintDeferrability.InitiallyImmediate)) {

						// For each row added to this column
						foreach (int rowIndex in rowIndices) {
							if (!IsUniqueColumns(table, rowIndex, primaryKey.ColumnNames, false)) {
								throw new ConstraintViolationException(
									SqlModelErrorCodes.PrimaryKeyViolation,
									deferred.AsDebugString() + " primary Key constraint violation (" +
									primaryKey.ConstraintName + ") Columns = ( " +
									String.Join(", ", primaryKey.ColumnNames) +
									" ) Table = ( " + tableName + " )");
							}
						} // For each row being added
					}

					// Check any unique constraints.
					var uniqueConstraints = transaction.QueryTableUniqueKeys(tableName);
					foreach (var unique in uniqueConstraints) {
						if (deferred == ConstraintDeferrability.InitiallyDeferred ||
						    unique.Deferred == ConstraintDeferrability.InitiallyImmediate) {

							// For each row added to this column
							foreach (int rowIndex in rowIndices) {
								if (!IsUniqueColumns(table, rowIndex, unique.ColumnNames, true)) {
									throw new ConstraintViolationException(
										SqlModelErrorCodes.UniqueViolation,
										deferred.AsDebugString() + " unique constraint violation (" +
										unique.ConstraintName + ") Columns = ( " +
										String.Join(", ", unique.ColumnNames) + " ) Table = ( " +
										tableName + " )");
								}
							} // For each row being added
						}
					}

					// Check any foreign key constraints.
					// This ensures all foreign references in the table are referenced
					// to valid records.
					var foreignConstraints = transaction.QueryTableForeignKeys(tableName);

					foreach (var reference in foreignConstraints) {
						if (deferred == ConstraintDeferrability.InitiallyDeferred ||
						    reference.Deferred == ConstraintDeferrability.InitiallyImmediate) {
							// For each row added to this column
							foreach (int rowIndex in rowIndices) {
								// Make sure the referenced record exists

								// Return the count of records where the given row of
								//   table_name(columns, ...) IN
								//                    ref_table_name(ref_columns, ...)
								int rowCount = RowCountOfReferenceTable(transaction,
									rowIndex,
									reference.TableName, reference.ColumnNames,
									reference.ForeignTable, reference.ForeignColumnNames,
									false);
								if (rowCount == -1) {
									// foreign key is NULL
								}

								if (rowCount == 0) {
									throw new ConstraintViolationException(
										SqlModelErrorCodes.ForeignKeyViolation,
										deferred.AsDebugString() + " foreign key constraint violation (" +
										reference.ConstraintName + ") Columns = " +
										reference.TableName + "( " +
										String.Join(", ", reference.ColumnNames) + " ) -> " +
										reference.ForeignTable + "( " +
										String.Join(", ", reference.ForeignColumnNames) + " )");
								}
							} // For each row being added.
						}
					}

					// Any general checks of the inserted data
					var checkConstraints = transaction.QueryTableCheckExpressions(tableName);

					// For each check constraint, check that it evaluates to true.
					for (int i = 0; i < checkConstraints.Length; ++i) {
						var check = checkConstraints[i];
						if (deferred == ConstraintDeferrability.InitiallyDeferred ||
						    check.Deferred == ConstraintDeferrability.InitiallyImmediate) {

							// TODO: var exp = tableInfo.ResolveColumns(transaction.IgnoreIdentifierCase(), check.CheckExpression);
							var exp = tableInfo.ResolveColumns(true, check.CheckExpression);

							// For each row being added to this column
							for (int rn = 0; rn < rowIndices.Length; ++rn) {
								var resolver = new TableRowVariableResolver(table, rowIndices[rn]);
								var evalExp = exp.Evaluate(queryContext, resolver, null);
								var ob = ((SqlConstantExpression) evalExp).Value;

								var b = ob.AsBoolean();

								if (!b.IsNull) {
									if (b) {
										// Evaluated to false so don't allow this row to be added.
										throw new ConstraintViolationException(
											SqlModelErrorCodes.CheckViolation,
											deferred.AsDebugString() + " check constraint violation (" +
											check.ConstraintName + ") - '" + exp +
											"' evaluated to false for inserted/updated row.");
									}
								} else {
									// NOTE: This error will pass the row by default
									// TODO: emit a warning
								}
							}
						}
					}
				}
			}
		}

		public static
			void CheckRemoveConstraintViolations(this ITransaction transaction, ITable table, int[] rowIndices,
				ConstraintDeferrability deferred) {
			// Quick exit case
			if (rowIndices == null || rowIndices.Length == 0)
				return;

			var tableInfo = table.TableInfo;
			var tableName = tableInfo.TableName;

			// Check any imported foreign key constraints.
			// This ensures that a referential reference can not be removed making
			// it invalid.
			var foreignConstraints = transaction.QueryTableImportedForeignKeys(tableName);
			foreach (var reference in foreignConstraints) {
				if (deferred == ConstraintDeferrability.InitiallyDeferred ||
				    reference.Deferred == ConstraintDeferrability.InitiallyImmediate) {
					// For each row removed from this column
					foreach (int rowIndex in rowIndices) {
						// Make sure the referenced record exists

						// Return the count of records where the given row of
						//   ref_table_name(columns, ...) IN
						//                    table_name(ref_columns, ...)
						int rowCount = RowCountOfReferenceTable(transaction,
							rowIndex,
							reference.ForeignTable, reference.ForeignColumnNames,
							reference.TableName, reference.ColumnNames,
							true);

						// There must be 0 references otherwise the delete isn't allowed to
						// happen.
						if (rowCount > 0) {
							throw new ConstraintViolationException(SqlModelErrorCodes.ForeignKeyViolation,
								deferred.AsDebugString() + " foreign key constraint violation " +
								"on delete (" +
								reference.ConstraintName + ") Columns = " +
								reference.TableName + "( " +
								String.Join(", ", reference.ColumnNames) + " ) -> " +
								reference.ForeignTable + "( " +
								String.Join(", ", reference.ForeignColumnNames) + " )");
						}
					}
				}
			}
		}

		public static ConstraintInfo[] QueryTableForeignKeys(this ITransaction transaction, ObjectName tableName) {
			var t = transaction.GetTable(SystemSchema.ForeignKeyInfoTableName);
			var t2 = transaction.GetTable(SystemSchema.ForeignKeyColumnsTableName);

			// Returns the list indexes where column 3 = table name
			//                            and column 2 = schema name
			var objTableName = Field.String(tableName.Name);
			var objSchema = Field.String(tableName.Parent.Name);
			var data = t.SelectRowsEqual(3, objTableName, 2, objSchema).ToList();

			var groups = new ConstraintInfo[data.Count];

			for (int i = 0; i < data.Count; ++i) {
				int rowIndex = data[i];

				// The foreign key id
				var id = t.GetValue(rowIndex, 0);

				// The referenced table
				var refTableName = new ObjectName(
					new ObjectName(t.GetValue(rowIndex, 4).Value.ToString()),
					t.GetValue(rowIndex, 5).Value.ToString());

				// Select all records with equal id
				var cols = t2.SelectRowsEqual(0, id).ToList();

				var name = t.GetValue(rowIndex, 1).Value.ToString();
				var updateRule = (ForeignKeyAction) ((SqlNumber) t.GetValue(rowIndex, 6).Value).ToInt32();
				var deleteRule = (ForeignKeyAction) ((SqlNumber) t.GetValue(rowIndex, 7).Value).ToInt32();
				var deferred = (ConstraintDeferrability) ((SqlNumber) t.GetValue(rowIndex, 8).Value).ToInt16();
				;

				int colsSize = cols.Count;
				string[] keyCols = new string[colsSize];
				string[] refCols = new string[colsSize];
				for (int n = 0; n < colsSize; ++n) {
					for (int p = 0; p < colsSize; ++p) {
						int colsIndex = cols[p];
						if (t2.GetValue(colsIndex, 3) == n) {
							keyCols[n] = t2.GetValue(colsIndex, 1).Value.ToString();
							refCols[n] = t2.GetValue(colsIndex, 2).Value.ToString();
							break;
						}
					}
				}

				var constraint = ConstraintInfo.ForeignKey(name, tableName, keyCols, refTableName, refCols);
				constraint.OnDelete = deleteRule;
				constraint.OnUpdate = updateRule;
				constraint.Deferred = deferred;

				groups[i] = constraint;
			}

			return groups;
		}

		public static ConstraintInfo[] QueryTableImportedForeignKeys(this ITransaction transaction, ObjectName refTableName) {
			var t = transaction.GetTable(SystemSchema.ForeignKeyInfoTableName);
			var t2 = transaction.GetTable(SystemSchema.ForeignKeyColumnsTableName);

			// Returns the list indexes where column 5 = ref table name
			//                            and column 4 = ref schema name
			var objRefTableName = Field.String(refTableName.Name);
			var objRefSchema = Field.String(refTableName.Parent.Name);
			var data = t.SelectRowsEqual(5, objRefTableName, 4, objRefSchema).ToArray();

			var groups = new ConstraintInfo[data.Length];

			for (int i = 0; i < data.Length; ++i) {
				int rowIndex = data[i];

				// The foreign key id
				var id = t.GetValue(rowIndex, 0);

				// The referencee table
				var schemaNamePart = t.GetValue(rowIndex, 2).AsVarChar().Value.ToString();
				var tableNamePart = t.GetValue(rowIndex, 3).AsVarChar().Value.ToString();
				var tableName = new ObjectName(new ObjectName(schemaNamePart), tableNamePart);

				// Select all records with equal id
				var cols = t2.SelectRowsEqual(0, id).ToArray();

				var name = t.GetValue(rowIndex, 1).AsVarChar().Value.ToString();

				var updateRule = (ForeignKeyAction) ((SqlNumber) t.GetValue(rowIndex, 6).AsBigInt().Value).ToInt32();
				var deleteRule = (ForeignKeyAction) ((SqlNumber) t.GetValue(rowIndex, 7).AsBigInt().Value).ToInt32();
				var deferred = (ConstraintDeferrability) ((SqlNumber) t.GetValue(rowIndex, 8).AsBigInt().Value).ToInt16();

				int colsSize = cols.Length;
				string[] keyCols = new string[colsSize];
				string[] refCols = new string[colsSize];
				for (int n = 0; n < colsSize; ++n) {
					for (int p = 0; p < colsSize; ++p) {
						int colsIndex = cols[p];
						if (t2.GetValue(colsIndex, 3) == n) {
							keyCols[n] = t2.GetValue(colsIndex, 1);
							refCols[n] = t2.GetValue(colsIndex, 2);
							break;
						}
					}
				}

				var constraint = ConstraintInfo.ForeignKey(name, tableName, keyCols, refTableName, refCols);
				constraint.OnDelete = deleteRule;
				constraint.OnUpdate = updateRule;
				constraint.Deferred = deferred;

				groups[i] = constraint;
			}

			return groups;
		}

		public static ConstraintInfo[] QueryTableUniqueKeys(this ITransaction transaction, ObjectName tableName) {
			var t = transaction.GetTable(SystemSchema.UniqueKeyInfoTableName);
			var t2 = transaction.GetTable(SystemSchema.UniqueKeyColumnsTableName);

			// Returns the list indexes where column 3 = table name
			//                            and column 2 = schema name
			var objTableName = Field.String(tableName.Name);
			var objSchemaName = Field.String(tableName.Parent.Name);
			var data = t.SelectRowsEqual(3, objTableName, 2, objSchemaName).ToList();

			var constraints = new ConstraintInfo[data.Count];

			for (int i = 0; i < data.Count; ++i) {
				var id = t.GetValue(data[i], 0);

				// Select all records with equal id
				var cols = t2.SelectRowsEqual(0, id);

				var name = t.GetValue(data[i], 1).Value.ToString();
				var columns = ToColumns(t2, cols); // the list of columns
				var deferred = (ConstraintDeferrability) ((SqlNumber) t.GetValue(data[i], 4).Value).ToInt16();

				var constraint = ConstraintInfo.Unique(name, tableName, columns);
				constraint.Deferred = deferred;
				constraints[i] = constraint;
			}

			return constraints;
		}

		public static ConstraintInfo QueryTablePrimaryKey(this ITransaction transaction, ObjectName tableName) {
			var t = transaction.GetTable(SystemSchema.PrimaryKeyInfoTableName);
			var t2 = transaction.GetTable(SystemSchema.PrimaryKeyColumnsTableName);

			// Returns the list indexes where column 3 = table name
			//                            and column 2 = schema name
			var objTableName = Field.String(tableName.Name);
			var objSchemaName = Field.String(tableName.Parent.Name);
			var data = t.SelectRowsEqual(3, objTableName, 2, objSchemaName).ToList();

			if (data.Count > 1)
				throw new InvalidOperationException("Assertion failed: multiple primary key for: " + tableName);

			if (data.Count == 0)
				return null;

			int rowIndex = data[0];

			var id = t.GetValue(rowIndex, 0);

			// All columns with this id
			var list = t2.SelectRowsEqual(0, id);

			// Make it in to a columns object
			var name = t.GetValue(rowIndex, 1).AsVarChar().Value.ToString();
			string[] columns = ToColumns(t2, list);
			var deferred = (ConstraintDeferrability) ((SqlNumber) t.GetValue(rowIndex, 4).Value).ToInt16();

			var constraint = ConstraintInfo.PrimaryKey(name, tableName, columns);
			constraint.Deferred = deferred;
			return constraint;
		}

		public static ObjectName[] QueryTablesRelationallyLinkedTo(this ITransaction transaction, ObjectName tableName) {
			var list = new List<ObjectName>();
			var refs = QueryTableForeignKeys(transaction, tableName);
			foreach (var fkeyRef in refs) {
				var tname = fkeyRef.ForeignTable;
				if (!list.Contains(tname))
					list.Add(tname);
			}

			refs = QueryTableImportedForeignKeys(transaction, tableName);
			foreach (var fkeyRef in refs) {
				var tname = fkeyRef.TableName;
				if (!list.Contains(tname))
					list.Add(tname);
			}

			return list.ToArray();
		}

		public static ConstraintInfo[] QueryTableCheckExpressions(this ITransaction transaction, ObjectName tableName) {
			var t = transaction.GetTable(SystemSchema.CheckInfoTableName);

			// Returns the list indexes where column 3 = table name
			//                            and column 2 = schema name
			var objTableName = Field.String(tableName.Name);
			var objSchemaName = Field.String(tableName.Parent.Name);
			var data = t.SelectRowsEqual(3, objTableName, 2, objSchemaName).ToList();
			var checks = new ConstraintInfo[data.Count];

			for (int i = 0; i < checks.Length; ++i) {
				int rowIndex = data[i];

				string name = t.GetValue(rowIndex, 1).Value.ToString();
				var deferred = (ConstraintDeferrability) ((SqlNumber) t.GetValue(rowIndex, 5).Value).ToInt16();
				SqlExpression expression = null;

				// Is the deserialized version available?
				if (t.TableInfo.ColumnCount > 6) {
					var sexp = (SqlBinary) t.GetValue(rowIndex, 6).Value;
					if (!sexp.IsNull) {
						try {
							// Deserialize the expression
							// TODO: expression = (SqlExpression)ObjectTranslator.Deserialize(sexp);
							throw new NotImplementedException();
						} catch (Exception) {
							// We weren't able to deserialize the expression so report the
							// error to the log
							// TODO:
						}
					}
				}

				// Otherwise we need to parse it from the string
				if (expression == null) {
					expression = SqlExpression.Parse(t.GetValue(rowIndex, 4).Value.ToString());
				}

				var check = ConstraintInfo.Check(name, tableName, expression);
				check.Deferred = deferred;
				checks[i] = check;
			}

			return checks;
		}

		public static void DropAllTableConstraints(this ITransaction transaction, ObjectName tableName) {
			var primary = transaction.QueryTablePrimaryKey(tableName);
			var uniques = transaction.QueryTableUniqueKeys(tableName);
			var expressions = transaction.QueryTableCheckExpressions(tableName);
			var refs = transaction.QueryTableForeignKeys(tableName);

			if (primary != null)
				transaction.DropTablePrimaryKey(tableName, primary.ConstraintName);
			foreach (var unique in uniques) {
				transaction.DropTableUniqueKey(tableName, unique.ConstraintName);
			}
			foreach (var expression in expressions) {
				transaction.DropTableCheck(tableName, expression.ConstraintName);
			}
			foreach (var reference in refs) {
				transaction.DropTableForeignKey(tableName, reference.ConstraintName);
			}
		}

		public static int DropTableConstraint(this ITransaction transaction, ObjectName tableName, string constraintName) {
			int dropCount = 0;
			if (transaction.DropTablePrimaryKey(tableName, constraintName)) {
				++dropCount;
			}
			if (transaction.DropTableUniqueKey(tableName, constraintName)) {
				++dropCount;
			}
			if (transaction.DropTableCheck(tableName, constraintName)) {
				++dropCount;
			}
			if (transaction.DropTableForeignKey(tableName, constraintName)) {
				++dropCount;
			}
			return dropCount;
		}

		public static bool DropTablePrimaryKey(this ITransaction transaction, ObjectName tableName, string constraintName) {
			var t = transaction.GetMutableTable(SystemSchema.PrimaryKeyInfoTableName);
			var t2 = transaction.GetMutableTable(SystemSchema.PrimaryKeyColumnsTableName);
			IEnumerable<int> data;
			if (constraintName != null) {
				// Returns the list of indexes where column 1 = constraint name
				//                               and column 2 = schema name
				data = t.SelectRowsEqual(1, Field.String(constraintName), 2, Field.String(tableName.ParentName));
			} else {
				// Returns the list of indexes where column 3 = table name
				//                               and column 2 = schema name
				data = t.SelectRowsEqual(3, Field.String(tableName.Name), 2, Field.String(tableName.ParentName));
			}

			var resultList = data.ToList();

			if (resultList.Count > 1)
				throw new InvalidOperationException("Assertion failed: multiple primary key for: " + tableName);

			if (resultList.Count == 1) {
				int rowIndex = resultList[0];

				// The id
				var id = t.GetValue(rowIndex, 0);

				// All columns with this id
				var columns = t2.SelectRowsEqual(0, id);

				// Delete from the table
				t2.DeleteRows(columns);
				t.DeleteRows(resultList);
				return true;
			}

			return false;
		}

		public static bool DropTableUniqueKey(this ITransaction transaction, ObjectName table, string constraintName) {
			var t = transaction.GetMutableTable(SystemSchema.UniqueKeyInfoTableName);
			var t2 = transaction.GetMutableTable(SystemSchema.UniqueKeyColumnsTableName);

			// Returns the list of indexes where column 1 = constraint name
			//                               and column 2 = schema name
			var data = t.SelectRowsEqual(1, Field.String(constraintName), 2, Field.String(table.ParentName));

			var resultList = data.ToList();
			if (resultList.Count > 1)
				throw new InvalidOperationException("Assertion failed: multiple unique constraint name: " + constraintName);

			if (resultList.Count == 1) {
				var rowIndex = resultList[0];

				// The id
				var id = t.GetValue(rowIndex, 0);

				// All columns with this id
				var columns = t2.SelectRowsEqual(0, id);

				// Delete from the table
				t2.DeleteRows(columns);
				t.DeleteRows(resultList);
				return true;
			}

			return false;
		}

		public static bool DropTableCheck(this ITransaction transaction, ObjectName table, string constraintName) {
			var t = transaction.GetMutableTable(SystemSchema.CheckInfoTableName);

			// Returns the list of indexes where column 1 = constraint name
			//                               and column 2 = schema name
			var data = t.SelectRowsEqual(1, Field.String(constraintName), 2, Field.String(table.ParentName));
			var resultList = data.ToList();
			if (resultList.Count > 1)
				throw new InvalidOperationException("Assertion failed: multiple check constraint name: " + constraintName);

			if (resultList.Count == 1) {
				// Delete the check constraint
				t.DeleteRows(resultList);
				return true;
			}

			// data.size() == 0 so the constraint wasn't found
			return false;
		}

		public static bool DropTableForeignKey(this ITransaction transaction, ObjectName table, string constraintName) {
			var t = transaction.GetMutableTable(SystemSchema.ForeignKeyInfoTableName);
			var t2 = transaction.GetMutableTable(SystemSchema.ForeignKeyColumnsTableName);

			// Returns the list of indexes where column 1 = constraint name
			//                               and column 2 = schema name
			var data = t.SelectRowsEqual(1, Field.String(constraintName), 2, Field.String(table.ParentName));
			var resultList = data.ToList();

			if (resultList.Count > 1)
				throw new InvalidOperationException("Assertion failed: multiple foreign key constraint " + "name: " + constraintName);

			if (resultList.Count == 1) {
				int rowIndex = resultList[0];

				// The id
				var id = t.GetValue(rowIndex, 0);

				// All columns with this id
				var columns = t2.SelectRowsEqual(0, id);

				// Delete from the table
				t2.DeleteRows(columns);
				t.DeleteRows(resultList);
				return true;
			}

			// data.size() == 0 so the constraint wasn't found
			return false;
		}

		private sealed class TableRowVariableResolver : IVariableResolver {

			private readonly ITable table;
			private readonly int rowIndex;

			public TableRowVariableResolver(ITable table, int rowIndex) {
				this.table = table;
				this.rowIndex = rowIndex;
			}

			private int FindColumnName(ObjectName variable) {
				int colIndex = table.TableInfo.IndexOfColumn(variable.Name);
				if (colIndex == -1)
					throw new InvalidOperationException("Can't find column: " + variable);

				return colIndex;
			}

			// --- Implemented ---

			public int SetId {
				get { return rowIndex; }
			}

			public Field Resolve(ObjectName variable) {
				int colIndex = FindColumnName(variable);
				return table.GetValue(rowIndex, colIndex);
			}

			public SqlType ReturnType(ObjectName variable) {
				int colIndex = FindColumnName(variable);
				return table.TableInfo[colIndex].ColumnType;
			}
		}
	}
}