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

using Deveel.Data.DbSystem;
using Deveel.Diagnostics;

namespace Deveel.Data.Transactions {
	public static class TransactionExtensions {
		/// <summary>
		/// Convenience, given a <see cref="SimpleTableQuery"/> object this 
		/// will return a list of column names in sequence that represent the 
		/// columns in a group constraint.
		/// </summary>
		/// <param name="dt"></param>
		/// <param name="cols">The unsorted list of indexes in the table that 
		/// represent the group.</param>
		/// <remarks>
		/// Assumes column 2 of dt is the sequence number and column 1 is the name
		/// of the column.
		/// </remarks>
		/// <returns></returns>
		private static String[] ToColumns(SimpleTableQuery dt, IList<int> cols) {
			int size = cols.Count;
			String[] list = new String[size];

			// for each n of the output list
			for (int n = 0; n < size; ++n) {
				// for each i of the input list
				for (int i = 0; i < size; ++i) {
					int rowIndex = cols[i];
					int seqNo = ((BigNumber)dt.Get(2, rowIndex).Object).ToInt32();
					if (seqNo == n) {
						list[n] = dt.Get(1, rowIndex).Object.ToString();
						break;
					}
				}
			}

			return list;
		}

		/// <summary>
		/// Returns true if the schema exists within this transaction.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="name"></param>
		/// <returns></returns>
		public static bool SchemaExists(this ITransaction transaction, string name) {
			TableName tableName = SystemSchema.SchemaInfoTable;
			ITableDataSource t = transaction.GetTable(tableName);
			SimpleTableQuery dt = new SimpleTableQuery(t);

			// Returns true if there's a single entry in dt where column 1 = name
			try {
				return dt.Exists(1, name);
			} finally {
				dt.Dispose();
			}
		}

		/// <summary>
		/// Resolves the case of the given schema name if the database is 
		/// performing case insensitive identifier matching.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="ignoreCase"></param>
		/// <returns>
		/// Returns a SchemaDef object that identifiers the schema. 
		/// Returns null if the schema name could not be resolved.
		/// </returns>
		public static SchemaDef ResolveSchemaCase(this ITransaction transaction, string name, bool ignoreCase) {
			// The list of schema
			SimpleTableQuery dt = new SimpleTableQuery(transaction.GetTable(SystemSchema.SchemaInfoTable));

			try {
				IRowEnumerator e = dt.GetRowEnumerator();
				if (ignoreCase) {
					SchemaDef result = null;
					while (e.MoveNext()) {
						int rowIndex = e.RowIndex;
						String cur_name = dt.Get(1, rowIndex).Object.ToString();
						if (String.Compare(name, cur_name, true) == 0) {
							if (result != null)
								throw new StatementException("Ambiguous schema name: '" + name + "'");

							string type = dt.Get(2, rowIndex).Object.ToString();
							result = new SchemaDef(cur_name, type);
						}
					}
					return result;

				} else {  // if (!ignore_case)
					while (e.MoveNext()) {
						int rowIndex = e.RowIndex;
						string curName = dt.Get(1, rowIndex).Object.ToString();
						if (name.Equals(curName)) {
							string type = dt.Get(2, rowIndex).Object.ToString();
							return new SchemaDef(curName, type);
						}
					}
					// Not found
					return null;
				}
			} finally {
				dt.Dispose();
			}
		}

		/// <summary>
		/// Returns an array of <see cref="SchemaDef"/> objects for each schema 
		/// currently setup in the database.
		/// </summary>
		/// <returns></returns>
		public static SchemaDef[] GetSchemaList(this ITransaction transaction) {
			// The list of schema
			SimpleTableQuery dt = new SimpleTableQuery(transaction.GetTable(SystemSchema.SchemaInfoTable));
			IRowEnumerator e = dt.GetRowEnumerator();
			SchemaDef[] arr = new SchemaDef[dt.RowCount];
			int i = 0;

			while (e.MoveNext()) {
				int row_index = e.RowIndex;
				string cur_name = dt.Get(1, row_index).Object.ToString();
				string cur_type = dt.Get(2, row_index).Object.ToString();
				arr[i] = new SchemaDef(cur_name, cur_type);
				++i;
			}

			dt.Dispose();
			return arr;
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
		public static DataConstraintInfo[] QueryTableCheckExpressions(this ITransaction transaction, TableName tableName) {
			ITableDataSource t = transaction.GetTable(SystemSchema.CheckInfoTable);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table

			DataConstraintInfo[] checks;
			try {
				// Returns the list indexes where column 3 = table name
				//                            and column 2 = schema name
				IList<int> data = dt.SelectEqual(3, tableName.Name,
													2, tableName.Schema);
				checks = new DataConstraintInfo[data.Count];

				for (int i = 0; i < checks.Length; ++i) {
					int row_index = data[i];

					string name = dt.Get(1, row_index).Object.ToString();
					ConstraintDeferrability deferred = (ConstraintDeferrability)((BigNumber)dt.Get(5, row_index).Object).ToInt16();
					Expression expression = null;

					// Is the deserialized version available?
					if (t.TableInfo.ColumnCount > 6) {
						ByteLongObject sexp = (ByteLongObject)dt.Get(6, row_index).Object;
						if (sexp != null) {
							try {
								// Deserialize the expression
								expression = (Expression)ObjectTranslator.Deserialize(sexp);
							} catch (Exception e) {
								// We weren't able to deserialize the expression so report the
								// error to the log
								transaction.Context.Logger.Warning(transaction, "Unable to deserialize the check expression. The error is: " + e.Message);
								transaction.Context.Logger.Warning(transaction, "Parsing the check expression instead.");
							}
						}
					}
					// Otherwise we need to parse it from the string
					if (expression == null) {
						expression = Expression.Parse(dt.Get(4, row_index).Object.ToString());
					}

					DataConstraintInfo check = DataConstraintInfo.Check(name, expression);
					check.TableName = tableName;
					check.Deferred = deferred;
					checks[i] = check;
				}

			} finally {
				dt.Dispose();
			}

			return checks;
		}

		/// <summary>
		/// Returns an array of column references in the given table that represent
		/// foreign key references that reference columns in the given table.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="refTableName"></param>
		/// <remarks>
		/// This is a reverse mapping of the <see cref="QueryTableForeignKeys"/>
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
		public static DataConstraintInfo[] QueryTableImportedForeignKeys(this ITransaction transaction, TableName refTableName) {
			ITableDataSource t = transaction.GetTable(SystemSchema.ForeignInfoTable);
			ITableDataSource t2 = transaction.GetTable(SystemSchema.ForeignColsTable);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			DataConstraintInfo[] groups;
			try {
				// Returns the list indexes where column 5 = ref table name
				//                            and column 4 = ref schema name
				IList<int> data = dt.SelectEqual(5, refTableName.Name,
												 4, refTableName.Schema);

				groups = new DataConstraintInfo[data.Count];

				for (int i = 0; i < data.Count; ++i) {
					int rowIndex = data[i];

					// The foreign key id
					TObject id = dt.Get(0, rowIndex);

					// The referencee table
					TableName tableName = new TableName(
						  dt.Get(2, rowIndex).Object.ToString(),
						  dt.Get(3, rowIndex).Object.ToString());

					// Select all records with equal id
					IList<int> cols = dtcols.SelectEqual(0, id);

					string name = dt.Get(1, rowIndex).Object.ToString();
					ConstraintAction updateRule = (ConstraintAction)dt.Get(6, rowIndex).ToBigNumber().ToInt32();
					ConstraintAction deleteRule = (ConstraintAction)dt.Get(7, rowIndex).ToBigNumber().ToInt32();
					ConstraintDeferrability deferred = (ConstraintDeferrability)((BigNumber)dt.Get(8, rowIndex).Object).ToInt16();

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

					DataConstraintInfo constraint = DataConstraintInfo.ForeignKey(name, keyCols, refTableName, refCols,
																				  deleteRule, updateRule);
					constraint.TableName = tableName;
					constraint.Deferred = deferred;

					groups[i] = constraint;
				}
			} finally {
				dt.Dispose();
				dtcols.Dispose();
			}

			return groups;
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
		public static DataConstraintInfo[] QueryTableForeignKeys(this ITransaction transaction, TableName tableName) {
			ITableDataSource t = transaction.GetTable(SystemSchema.ForeignInfoTable);
			ITableDataSource t2 = transaction.GetTable(SystemSchema.ForeignColsTable);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			DataConstraintInfo[] groups;
			try {
				// Returns the list indexes where column 3 = table name
				//                            and column 2 = schema name
				IList<int> data = dt.SelectEqual(3, tableName.Name,
												 2, tableName.Schema);

				groups = new DataConstraintInfo[data.Count];

				for (int i = 0; i < data.Count; ++i) {
					int rowIndex = data[i];

					// The foreign key id
					TObject id = dt.Get(0, rowIndex);

					// The referenced table
					TableName refTableName = new TableName(
							   dt.Get(4, rowIndex).Object.ToString(),
							   dt.Get(5, rowIndex).Object.ToString());

					// Select all records with equal id
					IList<int> cols = dtcols.SelectEqual(0, id);

					string name = dt.Get(1, rowIndex).Object.ToString();
					ConstraintAction updateRule = (ConstraintAction)dt.Get(6, rowIndex).ToBigNumber().ToInt32();
					ConstraintAction deleteRule = (ConstraintAction)dt.Get(7, rowIndex).ToBigNumber().ToInt32();
					ConstraintDeferrability deferred = (ConstraintDeferrability)((BigNumber)dt.Get(8, rowIndex).Object).ToInt16();

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

					DataConstraintInfo constraint = DataConstraintInfo.ForeignKey(name, keyCols, refTableName, refCols,
																				  deleteRule, updateRule);
					constraint.TableName = tableName;
					constraint.Deferred = deferred;

					groups[i] = constraint;
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
		public static DataConstraintInfo QueryTablePrimaryKey(this ITransaction transaction, TableName tableName) {
			ITableDataSource t = transaction.GetTable(SystemSchema.PrimaryInfoTable);
			ITableDataSource t2 = transaction.GetTable(SystemSchema.PrimaryColsTable);
			SimpleTableQuery dt = new SimpleTableQuery(t); // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2); // The columns

			try {
				// Returns the list indexes where column 3 = table name
				//                            and column 2 = schema name
				IList<int> data = dt.SelectEqual(3, tableName.Name,
												 2, tableName.Schema);

				if (data.Count > 1)
					throw new ApplicationException("Assertion failed: multiple primary key for: " + tableName);

				if (data.Count == 0)
					return null;

				int rowIndex = data[0];
				// The id
				TObject id = dt.Get(0, rowIndex);
				// All columns with this id
				IList<int> list = dtcols.SelectEqual(0, id);
				// Make it in to a columns object
				string name = dt.Get(1, rowIndex).Object.ToString();
				string[] columns = ToColumns(dtcols, list);
				ConstraintDeferrability deferred = (ConstraintDeferrability)((BigNumber)dt.Get(4, rowIndex).Object).ToInt16();

				DataConstraintInfo constraint = DataConstraintInfo.PrimaryKey(name, columns);
				constraint.TableName = tableName;
				constraint.Deferred = deferred;
				return constraint;

			} finally {
				dt.Dispose();
				dtcols.Dispose();
			}
		}

		/// <summary>
		/// Returns the list of tables (as a TableName array) that are dependant
		/// on the data in the given table to maintain referential consistancy.
		/// </summary>
		/// <param name="transaction"></param>
		/// <param name="tableName"></param>
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
		public static TableName[] QueryTablesRelationallyLinkedTo(this ITransaction transaction, TableName tableName) {
			List<TableName> list = new List<TableName>();
			DataConstraintInfo[] refs = QueryTableForeignKeys(transaction, tableName);
			foreach (DataConstraintInfo fkeyRef in refs) {
				TableName tname = fkeyRef.ReferencedTableName;
				if (!list.Contains(tname))
					list.Add(tname);
			}

			refs = QueryTableImportedForeignKeys(transaction, tableName);
			foreach (DataConstraintInfo fkeyRef in refs) {
				TableName tname = fkeyRef.TableName;
				if (!list.Contains(tname))
					list.Add(tname);
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
		public static DataConstraintInfo[] QueryTableUniques(this ITransaction transaction, TableName tableName) {
			ITableDataSource t = transaction.GetTable(SystemSchema.UniqueInfoTable);
			ITableDataSource t2 = transaction.GetTable(SystemSchema.UniqueColsTable);
			SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
			SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

			DataConstraintInfo[] constraints;
			try {
				// Returns the list indexes where column 3 = table name
				//                            and column 2 = schema name
				IList<int> data = dt.SelectEqual(3, tableName.Name,
												 2, tableName.Schema);

				constraints = new DataConstraintInfo[data.Count];

				for (int i = 0; i < data.Count; ++i) {
					TObject id = dt.Get(0, data[i]);

					// Select all records with equal id
					IList<int> cols = dtcols.SelectEqual(0, id);

					string name = dt.Get(1, data[i]).Object.ToString();
					string[] columns = ToColumns(dtcols, cols);   // the list of columns
					ConstraintDeferrability deferred = (ConstraintDeferrability)((BigNumber)dt.Get(4, data[i]).Object).ToInt16();

					DataConstraintInfo constraint = DataConstraintInfo.Unique(name, columns);
					constraint.TableName = tableName;
					constraint.Deferred = deferred;
					constraints[i] = constraint;
				}
			} finally {
				dt.Dispose();
				dtcols.Dispose();
			}

			return constraints;
		}

		/// <summary>
		/// Returns the value of the persistent variable with the given name 
		/// or null if it doesn't exist.
		/// </summary>
		/// <param name="variable"></param>
		/// <returns></returns>
		public static string GetPersistantVariable(this ITransaction transaction, string variable) {
			TableName tableName = SystemSchema.PersistentVarTable;
			ITableDataSource t = transaction.GetTable(tableName);
			var dt = new SimpleTableQuery(t);
			String val = dt.GetVariable(1, 0, variable).ToString();
			dt.Dispose();
			return val;
		}
	}
}