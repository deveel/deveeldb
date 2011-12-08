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
using System.Text;

using Deveel.Diagnostics;

namespace Deveel.Data {
	public sealed partial class TableDataConglomerate {
		/// <summary>
		/// Converts a String[] array to a comma deliminated string list.
		/// </summary>
		/// <param name="list"></param>
		/// <returns></returns>
		internal static string StringColumnList(string[] list) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < list.Length - 1; ++i) {
				sb.Append(list[i]);
			}
			sb.Append(list[list.Length - 1]);
			return sb.ToString();
		}

		/// <summary>
		/// Returns either 'Immediate' or 'Deferred' dependant on the deferred short.
		/// </summary>
		/// <param name="deferred"></param>
		/// <returns></returns>
		internal static String DeferredString(ConstraintDeferrability deferred) {
			switch (deferred) {
				case (ConstraintDeferrability.InitiallyImmediate):
					return "Immediate";
				case (ConstraintDeferrability.InitiallyDeferred):
					return "Deferred";
				default:
					throw new ApplicationException("Unknown deferred string.");
			}
		}

		/// <summary>
		/// Returns a list of column indices into the given <see cref="DataTableDef"/>
		/// for the given column names.
		/// </summary>
		/// <param name="tableDef"></param>
		/// <param name="cols"></param>
		/// <returns></returns>
		internal static int[] FindColumnIndices(DataTableDef tableDef, string[] cols) {
			// Resolve the list of column names to column indexes
			int[] colIndexes = new int[cols.Length];
			for (int i = 0; i < cols.Length; ++i) {
				colIndexes[i] = tableDef.FindColumnName(cols[i]);
			}
			return colIndexes;
		}

		/// <summary>
		/// Checks the uniqueness of the columns in the row of the table.
		/// </summary>
		/// <param name="table"></param>
		/// <param name="rindex"></param>
		/// <param name="cols"></param>
		/// <param name="nullsAllowed"></param>
		/// <remarks>
		/// If the given column information in the row data is not unique then 
		/// it returns false. We also check for a NULL values - a PRIMARY KEY 
		/// constraint does not allow NULL values, whereas a UNIQUE constraint 
		/// does.
		/// </remarks>
		/// <returns></returns>
		private static bool IsUniqueColumns(ITableDataSource table, int rindex, string[] cols, bool nullsAllowed) {
			DataTableDef tableDef = table.TableInfo;
			// 'identical_rows' keeps a tally of the rows that match our added cell.
			IList<int> identicalRows = null;

			// Resolve the list of column names to column indexes
			int[] colIndexes = FindColumnIndices(tableDef, cols);

			// If the value being tested for uniqueness contains NULL, we return true
			// if nulls are allowed.
			for (int i = 0; i < colIndexes.Length; ++i) {
				TObject cell = table.GetCellContents(colIndexes[i], rindex);
				if (cell.IsNull)
					return nullsAllowed;
			}


			for (int i = 0; i < colIndexes.Length; ++i) {
				int colIndex = colIndexes[i];

				// Get the cell being inserted,
				TObject cell = table.GetCellContents(colIndex, rindex);

				// We are assured of uniqueness if 'identicalRows != null &&
				// identicalRows.Count == 0'  This is because 'identicalRows' keeps
				// a running tally of the rows in the table that contain unique columns
				// whose cells match the record being added.

				if (identicalRows == null || identicalRows.Count > 0) {
					// Ask SelectableScheme to return pointers to row(s) if there is
					// already a cell identical to this in the table.

					SelectableScheme ss = table.GetColumnScheme(colIndex);
					List<int> ivec = new List<int>(ss.SelectEqual(cell));

					// If 'identical_rows' hasn't been set up yet then set it to 'ivec'
					// (the list of rows where there is a cell which is equal to the one
					//  being added)
					// If 'identical_rows' has been set up, then perform an
					// 'intersection' operation on the two lists (only keep the numbers
					// that are repeated in both lists).  Therefore we keep the rows
					// that match the row being added.

					if (identicalRows == null) {
						identicalRows = ivec;
					} else {
						ivec.Sort();
						int rowIndex = identicalRows.Count - 1;
						while (rowIndex >= 0) {
							int val = identicalRows[rowIndex];
							int foundIndex = ivec.BinarySearch(val);
							// If we _didn't_ find the index in the array
							if (foundIndex < 0 ||
								ivec[foundIndex] != val) {
								identicalRows.RemoveAt(rowIndex);
							}
							--rowIndex;
						}
					}
				}
			} // for each column

			// If there is 1 (the row we added) then we are unique, otherwise we are
			// not.
			if (identicalRows != null) {
				int sz = identicalRows.Count;
				if (sz == 1)
					return true;
				if (sz > 1)
					return false;
				if (sz == 0)
					throw new ApplicationException("Assertion failed: We must be able to find the " +
					                               "row we are testing uniqueness against!");
			}
			return true;

		}


		/// <summary>
		/// Returns the key indices found in the given table.
		/// </summary>
		/// <param name="t2"></param>
		/// <param name="col2Indexes"></param>
		/// <param name="keyValue"></param>
		/// <remarks>
		/// The keys are in the given column indices, and the key is in the 
		/// 'key' array. This can be used to count the number of keys found 
		/// in a table for constraint violation checking.
		/// </remarks>
		/// <returns></returns>
		internal static IList<int> FindKeys(ITableDataSource t2, int[] col2Indexes, TObject[] keyValue) {
			int keySize = keyValue.Length;

			// Now command table 2 to determine if the key values are present.
			// Use index scan on first key.
			SelectableScheme ss = t2.GetColumnScheme(col2Indexes[0]);
			IList<int> list = ss.SelectEqual(keyValue[0]);
			if (keySize > 1) {
				// Full scan for the rest of the columns
				int sz = list.Count;
				// For each element of the list
				for (int i = sz - 1; i >= 0; --i) {
					int rIndex = list[i];
					// For each key in the column list
					for (int c = 1; c < keySize; ++c) {
						int colIndex = col2Indexes[c];
						TObject cValue = keyValue[c];
						if (cValue.CompareTo(t2.GetCellContents(colIndex, rIndex)) != 0) {
							// If any values in the key are not equal set this flag to false
							// and remove the index from the list.
							list.RemoveAt(i);
							// Break the for loop
							break;
						}
					}
				}
			}

			return list;
		}

		/// <summary>
		/// Finds the number of rows that are referenced between the given 
		/// row of <paramref name="table1"/> and that match <paramref name="table2"/>.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="rowIndex"></param>
		/// <param name="table1"></param>
		/// <param name="cols1"></param>
		/// <param name="table2"></param>
		/// <param name="cols2"></param>
		/// <param name="checkSourceTableKey"></param>
		/// <remarks>
		/// This method is used to determine if there are referential links.
		/// <para>
		/// If this method returns -1 it means the value being searched for is 
		/// <c>NULL</c> therefore we can't determine if there are any referenced 
		/// links.
		/// </para>
		/// <para>
		/// <b>Hack</b>: If <paramref name="checkSourceTableKey"/> is set then the 
		/// key is checked for in the source table and if it exists returns 0. 
		/// Otherwise it looks for references to the key in table2.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		private static int RowCountOfReferenceTable(SimpleTransaction transaction,
					   int rowIndex, TableName table1, String[] cols1,
									  TableName table2, String[] cols2,
									  bool checkSourceTableKey) {

			// Get the tables
			ITableDataSource t1 = transaction.GetTableDataSource(table1);
			ITableDataSource t2 = transaction.GetTableDataSource(table2);
			// The table defs
			DataTableDef dtd1 = t1.TableInfo;
			DataTableDef dtd2 = t2.TableInfo;
			// Resolve the list of column names to column indexes
			int[] col1Indexes = FindColumnIndices(dtd1, cols1);
			int[] col2Indexes = FindColumnIndices(dtd2, cols2);

			int keySize = col1Indexes.Length;
			// Get the data from table1
			TObject[] keyValue = new TObject[keySize];
			int nullCount = 0;
			for (int n = 0; n < keySize; ++n) {
				keyValue[n] = t1.GetCellContents(col1Indexes[n], rowIndex);
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
				IList<int> keys = FindKeys(t1, col1Indexes, keyValue);
				int keyCount = keys.Count;
				if (keyCount > 0)
					return 0;
			}

			return FindKeys(t2, col2Indexes, keyValue).Count;
		}


		/// <summary>
		/// Checks that the nullibility and class of the fields in the given
		/// rows are valid.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="table"></param>
		/// <param name="rowIndices"></param>
		/// <remarks>
		/// Should be used as part of the insert procedure.
		/// </remarks>
		internal static void CheckFieldConstraintViolations(SimpleTransaction transaction, ITableDataSource table, int[] rowIndices) {
			// Quick exit case
			if (rowIndices == null || rowIndices.Length == 0)
				return;

			// Check for any bad cells - which are either cells that are 'null' in a
			// column declared as 'not null', or duplicated in a column declared as
			// unique.

			DataTableDef tableDef = table.TableInfo;

			// Check not-null columns are not null.  If they are null, throw an
			// error.  Additionally check that OBJECT columns are correctly
			// typed.

			// Check each field of the added rows
			int len = tableDef.ColumnCount;
			for (int i = 0; i < len; ++i) {
				// Get the column definition and the cell being inserted,
				DataTableColumnDef columnDef = tableDef[i];
				// For each row added to this column
				for (int rn = 0; rn < rowIndices.Length; ++rn) {
					TObject cell = table.GetCellContents(i, rowIndices[rn]);

					// Check: Column defined as not null and cell being inserted is
					// not null.
					if (columnDef.IsNotNull && cell.IsNull) {
						throw new DatabaseConstraintViolationException(
							DatabaseConstraintViolationException.NullableViolation,
							"You tried to add 'null' cell to column '" +
							tableDef[i].Name +
							"' which is declared as 'not_null'");
					}

					// Check: If column is an object, then deserialize and check the
					//        object is an instance of the class constraint,
					if (!cell.IsNull &&
						columnDef.SqlType == SqlType.Object) {
						string classConstraint = columnDef.TypeConstraintString;
						// Everything is derived from System.Object so this optimization
						// will not cause an object deserialization.
						if (!classConstraint.Equals("System.Object")) {
							// Get the binary representation of the object
							ByteLongObject serializedJobject = (ByteLongObject)cell.Object;
							// Deserialize the object
							object ob = ObjectTranslator.Deserialize(serializedJobject);
							// Check it's assignable from the constraining class
							if (!ob.GetType().IsAssignableFrom(columnDef.TypeConstraint)) {
								throw new DatabaseConstraintViolationException(
								  DatabaseConstraintViolationException.ObjectTypeViolation,
								  "The object being inserted is not derived from the " +
								  "class constraint defined for the column (" +
								  classConstraint + ")");
							}
						}
					}
				} // For each row being added
			} // for each column
		}

		/// <summary>
		/// Performs constraint violation checks on an addition of the given 
		/// set of row indices into the <see cref="ITableDataSource"/> in the
		/// given transaction.
		/// </summary>
		/// <param name="transaction">The <see cref="Transaction"/> instance 
		/// used to determine table  constraints.</param>
		/// <param name="table">The table to test.</param>
		/// <param name="rowIndices">The list of rows that were added to the 
		/// table.</param>
		/// <param name="deferred"></param>
		/// <remarks>
		/// If deferred is <see cref="ConstraintDeferrability.InitiallyImmediate"/>
		/// only immediate constraints are tested. If deferred  is
		/// <see cref="ConstraintDeferrability.InitiallyDeferred"/> all constraints 
		/// are tested.
		/// </remarks>
		/// <exception cref="DatabaseConstraintViolationException">
		/// If a violation is detected.
		/// </exception>
		internal static void CheckAddConstraintViolations(SimpleTransaction transaction, ITableDataSource table, int[] rowIndices, ConstraintDeferrability deferred) {
			string curSchema = table.TableInfo.Schema;
			IQueryContext context = new SystemQueryContext(transaction, curSchema);

			// Quick exit case
			if (rowIndices == null || rowIndices.Length == 0)
				return;

			DataTableDef tableDef = table.TableInfo;
			TableName tableName = tableDef.TableName;

			// ---- Constraint checking ----

			// Check any primary key constraint.
			Transaction.ColumnGroup primaryKey = Transaction.QueryTablePrimaryKeyGroup(transaction, tableName);
			if (primaryKey != null &&
				(deferred == ConstraintDeferrability.InitiallyDeferred ||
				 primaryKey.deferred == ConstraintDeferrability.InitiallyImmediate)) {

				// For each row added to this column
				foreach (int rowIndex in rowIndices) {
					if (!IsUniqueColumns(table, rowIndex, primaryKey.columns, false)) {
						throw new DatabaseConstraintViolationException(
						  DatabaseConstraintViolationException.PrimaryKeyViolation,
						  DeferredString(deferred) + " primary Key constraint violation (" +
						  primaryKey.name + ") Columns = ( " +
						  StringColumnList(primaryKey.columns) +
						  " ) Table = ( " + tableName + " )");
					}
				} // For each row being added
			}

			// Check any unique constraints.
			Transaction.ColumnGroup[] uniqueConstraints =
						  Transaction.QueryTableUniqueGroups(transaction, tableName);
			foreach (Transaction.ColumnGroup unique in uniqueConstraints) {
				if (deferred == ConstraintDeferrability.InitiallyDeferred ||
					unique.deferred == ConstraintDeferrability.InitiallyImmediate) {

					// For each row added to this column
					foreach (int rowIndex in rowIndices) {
						if (!IsUniqueColumns(table, rowIndex, unique.columns, true)) {
							throw new DatabaseConstraintViolationException(
							  DatabaseConstraintViolationException.UniqueViolation,
							  DeferredString(deferred) + " unique constraint violation (" +
							  unique.name + ") Columns = ( " +
							  StringColumnList(unique.columns) + " ) Table = ( " +
							  tableName + " )");
						}
					} // For each row being added
				}
			}

			// Check any foreign key constraints.
			// This ensures all foreign references in the table are referenced
			// to valid records.
			Transaction.ColumnGroupReference[] foreignConstraints =
				  Transaction.QueryTableForeignKeyReferences(transaction, tableName);

			foreach (Transaction.ColumnGroupReference reference in foreignConstraints) {
				if (deferred == ConstraintDeferrability.InitiallyDeferred ||
					reference.deferred == ConstraintDeferrability.InitiallyImmediate) {
					// For each row added to this column
					foreach (int rowIndex in rowIndices) {
						// Make sure the referenced record exists

						// Return the count of records where the given row of
						//   table_name(columns, ...) IN
						//                    ref_table_name(ref_columns, ...)
						int rowCount = RowCountOfReferenceTable(transaction,
												   rowIndex,
												   reference.key_table_name, reference.key_columns,
												   reference.ref_table_name, reference.ref_columns,
												   false);
						if (rowCount == -1) {
							// foreign key is NULL
						}
						if (rowCount == 0) {
							throw new DatabaseConstraintViolationException(
							  DatabaseConstraintViolationException.ForeignKeyViolation,
							  DeferredString(deferred) + " foreign key constraint violation (" +
							  reference.name + ") Columns = " +
							  reference.key_table_name + "( " +
							  StringColumnList(reference.key_columns) + " ) -> " +
							  reference.ref_table_name + "( " +
							  StringColumnList(reference.ref_columns) + " )");
						}
					} // For each row being added.
				}
			}

			// Any general checks of the inserted data
			Transaction.CheckExpression[] checkConstraints =
					   Transaction.QueryTableCheckExpressions(transaction, tableName);

			// The TransactionSystem object
			TransactionSystem system = transaction.System;

			// For each check constraint, check that it evaluates to true.
			for (int i = 0; i < checkConstraints.Length; ++i) {
				Transaction.CheckExpression check = checkConstraints[i];
				if (deferred == ConstraintDeferrability.InitiallyDeferred ||
					check.deferred == ConstraintDeferrability.InitiallyImmediate) {

					check = system.PrepareTransactionCheckConstraint(tableDef, check);
					Expression exp = check.expression;

					// For each row being added to this column
					for (int rn = 0; rn < rowIndices.Length; ++rn) {
						TableRowVariableResolver resolver = new TableRowVariableResolver(table, rowIndices[rn]);
						TObject ob = exp.Evaluate(null, resolver, context);
						bool? b = ob.ToNullableBoolean();

						if (b.HasValue) {
							if (!b.Value) {
								// Evaluated to false so don't allow this row to be added.
								throw new DatabaseConstraintViolationException(
								   DatabaseConstraintViolationException.CheckViolation,
								   DeferredString(deferred) + " check constraint violation (" +
								   check.name + ") - '" + exp.Text +
								   "' evaluated to false for inserted/updated row.");
							}
						} else {
							// NOTE: This error will pass the row by default
							transaction.Debug.Write(DebugLevel.Error,
										typeof(TableDataConglomerate),
										DeferredString(deferred) + " check constraint violation (" +
										check.name + ") - '" + exp.Text +
										"' returned a non boolean or NULL result.");
						}
					} // For each row being added
				}
			}
		}

		/// <summary>
		/// Performs constraint violation checks on an addition of the given 
		/// set of row indices into the <see cref="ITableDataSource"/> in the 
		/// given transaction.
		/// </summary>
		/// <param name="transaction">The <see cref="Transaction"/> instance 
		/// used to determine table  constraints.</param>
		/// <param name="table">The table to test.</param>
		/// <param name="rowIndex">The row that was added to the table.</param>
		/// <param name="deferred"></param>
		/// <remarks>
		/// If deferred is <see cref="ConstraintDeferrability.InitiallyImmediate"/>
		/// only immediate constraints are tested. If deferred  is
		/// <see cref="ConstraintDeferrability.InitiallyDeferred"/> all constraints 
		/// are tested.
		/// </remarks>
		/// <exception cref="DatabaseConstraintViolationException">
		/// If a violation is detected.
		/// </exception>
		internal static void CheckAddConstraintViolations(SimpleTransaction transaction, ITableDataSource table, int rowIndex, ConstraintDeferrability deferred) {
			CheckAddConstraintViolations(transaction, table, new int[] { rowIndex }, deferred);
		}

		/// <summary>
		/// Performs constraint violation checks on a removal of the given set 
		/// of row indexes from the <see cref="ITableDataSource"/> in the given 
		/// transaction.
		/// </summary>
		/// <param name="transaction">The <see cref="Transaction"/> instance used 
		/// to determine table constraints.</param>
		/// <param name="table">The table to test.</param>
		/// <param name="rowIndices">The list of rows that were removed from 
		/// the table.</param>
		/// <param name="deferred"></param>
		/// <remarks>
		/// If deferred is <see cref="ConstraintDeferrability.InitiallyImmediate"/>
		/// only immediate constraints are tested. If deferred  is
		/// <see cref="ConstraintDeferrability.InitiallyDeferred"/> all constraints 
		/// are tested.
		/// </remarks>
		/// <exception cref="DatabaseConstraintViolationException">
		/// If a violation is detected.
		/// </exception>
		internal static void CheckRemoveConstraintViolations(SimpleTransaction transaction, ITableDataSource table, int[] rowIndices, ConstraintDeferrability deferred) {
			// Quick exit case
			if (rowIndices == null || rowIndices.Length == 0)
				return;

			DataTableDef tableDef = table.TableInfo;
			TableName tableName = tableDef.TableName;

			// Check any imported foreign key constraints.
			// This ensures that a referential reference can not be removed making
			// it invalid.
			Transaction.ColumnGroupReference[] foreignConstraints =
				Transaction.QueryTableImportedForeignKeyReferences(transaction, tableName);
			foreach (Transaction.ColumnGroupReference reference in foreignConstraints) {
				if (deferred == ConstraintDeferrability.InitiallyDeferred ||
					reference.deferred == ConstraintDeferrability.InitiallyImmediate) {
					// For each row removed from this column
					foreach (int rowIndex in rowIndices) {
						// Make sure the referenced record exists

						// Return the count of records where the given row of
						//   ref_table_name(columns, ...) IN
						//                    table_name(ref_columns, ...)
						int rowCount = RowCountOfReferenceTable(transaction,
												   rowIndex,
												   reference.ref_table_name, reference.ref_columns,
												   reference.key_table_name, reference.key_columns,
												   true);
						// There must be 0 references otherwise the delete isn't allowed to
						// happen.
						if (rowCount > 0) {
							throw new DatabaseConstraintViolationException(
							  DatabaseConstraintViolationException.ForeignKeyViolation,
							  DeferredString(deferred) + " foreign key constraint violation " +
							  "on delete (" +
							  reference.name + ") Columns = " +
							  reference.key_table_name + "( " +
							  StringColumnList(reference.key_columns) + " ) -> " +
							  reference.ref_table_name + "( " +
							  StringColumnList(reference.ref_columns) + " )");
						}
					} // For each row being added.
				}
			}
		}

		/// <summary>
		/// Performs constraint violation checks on all the rows in the given
		/// table.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="table"></param>
		/// <param name="deferred"></param>
		/// <remarks>
		/// This method is useful when the constraint schema of a table changes 
		/// and we need to check existing data in a table is conformant with 
		/// the new constraint changes.
		/// <para>
		/// If deferred is <see cref="ConstraintDeferrability.InitiallyImmediate"/>
		/// only immediate constraints are tested. If deferred  is
		/// <see cref="ConstraintDeferrability.InitiallyDeferred"/> all constraints 
		/// are tested.
		/// </para>
		/// </remarks>
		/// <exception cref="DatabaseConstraintViolationException">
		/// If a violation is detected.
		/// </exception>
		static void CheckAllAddConstraintViolations(SimpleTransaction transaction, ITableDataSource table, ConstraintDeferrability deferred) {
			// Get all the rows in the table
			int[] rows = new int[table.RowCount];
			IRowEnumerator row_enum = table.GetRowEnumerator();
			int p = 0;
			while (row_enum.MoveNext()) {
				rows[p] = row_enum.RowIndex;
				++p;
			}
			// Check the constraints of all the rows in the table.
			CheckAddConstraintViolations(transaction, table, rows, ConstraintDeferrability.InitiallyDeferred);
		} 
	}
}