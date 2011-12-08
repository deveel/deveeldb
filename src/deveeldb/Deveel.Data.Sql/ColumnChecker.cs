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
using System.Collections;
using System.Collections.Generic;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A class that abstracts the checking of information in a table.
	/// </summary>
	/// <remarks>
	/// This is abstracted because the behaviour is shared between 
	/// <c>ALTER</c> and <c>CREATE</c> statement.
	/// </remarks>
	abstract class ColumnChecker {
		/// <summary>
		/// Given a column name string, this will strip off the preceeding 
		/// table name if there is one specified.
		/// </summary>
		/// <param name="table_domain"></param>
		/// <param name="column"></param>
		/// <remarks>
		/// For example,<c>Customer.id</c> would become <c>id</c>.  
		/// This also checks that the table specification is in the given 
		/// table domain.
		/// </remarks>
		/// <example>
		/// <code>
		/// StripTableName("Customer", "Customer.id");	// Correct
		/// StripTableName("Order", "Customer.di");		// Error
		/// </code>
		/// </example>
		/// <returns></returns>
		internal String StripTableName(String table_domain, String column) {
			if (column.IndexOf('.') != -1) {
				String st = table_domain + ".";
				if (!column.StartsWith(st)) {
					throw new StatementException("Column '" + column +
					  "' is not within the expected table domain '" + table_domain + "'");
				}
				column = column.Substring(st.Length);
			}
			return column;
		}

		/// <summary>
		/// Calls the <see cref="StripTableName"/> method on all elements 
		/// in the given list.
		/// </summary>
		/// <param name="table_domain"></param>
		/// <param name="column_list"></param>
		/// <returns></returns>
		internal ArrayList StripColumnList(String table_domain, ArrayList column_list) {
			if (column_list != null) {
				int size = column_list.Count;
				for (int i = 0; i < size; ++i) {
					String res = StripTableName(table_domain, (String)column_list[i]);
					column_list[i] = res;
				}
			}
			return column_list;
		}

		/// <summary>
		/// Resolves the given column name within the table.
		/// </summary>
		/// <param name="col_name"></param>
		/// <returns>
		/// Returns the resolved column name if the column exists within 
		/// the table being checked under, or <b>null</b> if it doesn't.
		/// </returns>
		/// <exception cref="StatementException">
		/// If the column name is abiguous reference.
		/// </exception>
		internal abstract String ResolveColumnName(String col_name);

		/// <summary>
		/// Resolves all the variables in the given expression.
		/// </summary>
		/// <param name="expression"></param>
		/// <remarks>
		/// This checks that all variables point to a column in the table 
		/// being created.
		/// </remarks>
		/// <exception cref="DatabaseException">
		/// If an error during the resolving process appened.
		/// </exception>
		internal void CheckExpression(Expression expression) {

			if (expression != null) {
				IList<VariableName> list = expression.AllVariables;
				for (int i = 0; i < list.Count; ++i) {
					VariableName v = list[i];
					String orig_col = v.Name;
					String resolved_column = ResolveColumnName(orig_col);
					if (resolved_column == null) {
						throw new DatabaseException("Column '" + orig_col +
													"' not found in the table.");
					}
					// Resolve the column name
					if (!orig_col.Equals(resolved_column)) {
						v.Name = resolved_column;
					}

				}

				// Don't allow select statements because they don't convert to a
				// text string that we can encode into the DataTableDef file.
				if (expression.HasSubQuery) {
					throw new DatabaseException("Sub-queries not permitted in " +
												"the check constraint expression.");
				}
			}

		}

		/// <summary>
		/// Checks all the columns in the given list.
		/// </summary>
		/// <param name="list"></param>
		/// <remarks>
		/// Additionally sets the entry with the correct column resolved.
		/// </remarks>
		/// <exception cref="StatementException">
		/// If any column names are not found in the columns in the 
		/// statement.
		/// </exception>
		internal void CheckColumnList(ArrayList list) {
			if (list != null) {
				for (int i = 0; i < list.Count; ++i) {
					String col = (String)list[i];
					String resolved_col = ResolveColumnName(col);
					if (resolved_col == null) {
						throw new DatabaseException(
											 "Column '" + col + "' not found the table.");
					}
					list[i] = resolved_col;
				}
			}
		}


		// ---------- Statics ----------

		/// <summary>
		/// Given a <see cref="DatabaseConnection"/> and a <see cref="TableName"/> 
		/// object, this returns an implementation of <see cref="ColumnChecker"/> 
		/// that is able to check that the column name exists in the table, 
		/// and that the reference is not ambigious.
		/// </summary>
		/// <param name="database"></param>
		/// <param name="tname"></param>
		/// <returns></returns>
		internal static ColumnChecker GetStandardColumnChecker(DatabaseConnection database, TableName tname) {
			DataTableDef table_def = database.GetTable(tname).TableInfo;
			bool ignores_case = database.IsInCaseInsensitiveMode;

			// Implement the checker
			return new StandardColumnChecker(table_def, ignores_case);
		}

		private class StandardColumnChecker : ColumnChecker {
			public StandardColumnChecker(DataTableDef table_def, bool ignore_cases) {
				this.table_def = table_def;
				this.ignores_case = ignore_cases;
			}

			private DataTableDef table_def;
			private bool ignores_case;

			internal override String ResolveColumnName(String col_name) {
				// We need to do case sensitive and case insensitive resolution,
				String found_col = null;
				for (int n = 0; n < table_def.ColumnCount; ++n) {
					DataTableColumnDef col =
										  (DataTableColumnDef)table_def[n];
					if (!ignores_case) {
						if (col.Name.Equals(col_name)) {
							return col_name;
						}
					} else {
						if (String.Compare(col.Name, col_name, true) == 0) {
							if (found_col != null) {
								throw new DatabaseException("Ambiguous column name '" +
															col_name + "'");
							}
							found_col = col.Name;
						}
					}
				}
				return found_col;
			}
		}
	}
}