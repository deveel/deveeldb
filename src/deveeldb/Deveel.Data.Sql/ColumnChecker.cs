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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A class that abstracts the checking of information in a table.
	/// </summary>
	/// <remarks>
	/// This is abstracted because the behaviour is shared between 
	/// <c>ALTER</c> and <c>CREATE</c> statement.
	/// </remarks>
	internal abstract class ColumnChecker {
		/// <summary>
		/// Given a column name string, this will strip off the preceeding 
		/// table name if there is one specified.
		/// </summary>
		/// <param name="tableDomain"></param>
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
		public string StripTableName(string tableDomain, string column) {
			if (column.IndexOf('.') != -1) {
				string st = tableDomain + ".";
				if (!column.StartsWith(st)) {
					throw new StatementException("Column '" + column +
					                             "' is not within the expected table domain '" + tableDomain + "'");
				}
				column = column.Substring(st.Length);
			}
			return column;
		}

		/// <summary>
		/// Calls the <see cref="StripTableName"/> method on all elements 
		/// in the given list.
		/// </summary>
		/// <param name="tableDomain"></param>
		/// <param name="columnList"></param>
		/// <returns></returns>
		public IList<string> StripColumnList(string tableDomain, IList<string> columnList) {
			if (columnList != null) {
				int size = columnList.Count;
				for (int i = 0; i < size; ++i) {
					string res = StripTableName(tableDomain, columnList[i]);
					columnList[i] = res;
				}
			}
			return columnList;
		}

		/// <summary>
		/// Resolves the given column name within the table.
		/// </summary>
		/// <param name="columnName"></param>
		/// <returns>
		/// Returns the resolved column name if the column exists within 
		/// the table being checked under, or <b>null</b> if it doesn't.
		/// </returns>
		/// <exception cref="StatementException">
		/// If the column name is abiguous reference.
		/// </exception>
		public abstract string ResolveColumnName(string columnName);

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
				// text string that we can encode into the DataTableInfo file.
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
		public void CheckColumnList(IList<string> list) {
			if (list != null) {
				for (int i = 0; i < list.Count; ++i) {
					string col = list[i];
					string resolvedCol = ResolveColumnName(col);
					if (resolvedCol == null)
						throw new DatabaseException("Column '" + col + "' not found the table.");

					list[i] = resolvedCol;
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
		public static ColumnChecker GetStandardColumnChecker(IDatabaseConnection database, TableName tname) {
			DataTableInfo tableInfo = database.GetTable(tname).TableInfo;
			bool ignoresCase = database.IsInCaseInsensitiveMode;

			// Implement the checker
			return new StandardColumnChecker(tableInfo, ignoresCase);
		}

		private class StandardColumnChecker : ColumnChecker {
			private readonly DataTableInfo tableInfo;
			private readonly bool ignoreCase;


			public StandardColumnChecker(DataTableInfo tableInfo, bool ignoreCase) {
				this.tableInfo = tableInfo;
				this.ignoreCase = ignoreCase;
			}

			public override String ResolveColumnName(string columnName) {
				// We need to do case sensitive and case insensitive resolution,
				string foundCol = null;
				for (int n = 0; n < tableInfo.ColumnCount; ++n) {
					DataColumnInfo col = tableInfo[n];
					if (String.Compare(col.Name, columnName, ignoreCase) == 0) {
						if (foundCol != null)
							throw new DatabaseException("Ambiguous column name '" + columnName + "'");

						foundCol = col.Name;
					}
				}
				return foundCol;
			}
		}
	}
}