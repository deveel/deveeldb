//  
//  ColumnChecker.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;
using System.Collections;

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
				IList list = expression.AllVariables;
				for (int i = 0; i < list.Count; ++i) {
					Variable v = (Variable)list[i];
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
			DataTableDef table_def = database.GetTable(tname).DataTableDef;
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