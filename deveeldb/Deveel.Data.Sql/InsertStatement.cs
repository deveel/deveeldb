//  
//  InsertStatement.cs
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

using Deveel.Data.Collections;

namespace Deveel.Data.Sql {
	/// <summary>
	/// The instance class that stores all the information about an 
	/// insert statement for processing.
	/// </summary>
	public class InsertStatement : Statement {

		String table_name;

		ArrayList col_list;

		ArrayList values_list;    //list contains List of elements to insert

		StatementTree select;

		ArrayList column_sets;

		bool from_values = false;

		bool from_select = false;

		bool from_set = false;

		// -----

		/// <summary>
		/// The table we are inserting stuff to.
		/// </summary>
		private DataTable insert_table;

		/// <summary>
		/// For 'from_values' and 'from_select', this is a list of indices into the
		/// 'insert_table' for the columns that we are inserting data into.
		/// </summary>
		private int[] col_index_list;

		/// <summary>
		/// The list of Variable objects the represent the list of columns being
		/// inserted into in this query.
		/// </summary>
		private Variable[] col_var_list;

		/// <summary>
		/// The TableName we are inserting into.
		/// </summary>
		private TableName tname;

		/// <summary>
		/// If this is a 'from_select' insert, the prepared Select object.
		/// </summary>
		private SelectStatement prepared_select;

		/// <summary>
		/// Tables that are relationally linked to the table being inserted 
		/// into, set after <see cref="Prepare"/>.
		/// </summary>
		/// <remarks>
		/// This is used to determine the tables we need to read lock because 
		/// we need to validate relational constraints on the tables.
		/// </remarks>
		private ArrayList relationally_linked_tables;


		// ---------- Implemented from Statement ----------

		public override void Prepare() {

			// Prepare this object from the StatementTree
			table_name = (String)cmd.GetObject("table_name");
			col_list = (ArrayList)cmd.GetObject("col_list");
			values_list = (ArrayList)cmd.GetObject("data_list");
			select = (StatementTree)cmd.GetObject("select");
			column_sets = (ArrayList)cmd.GetObject("assignments");
			String type = (String)cmd.GetObject("type");
			from_values = type.Equals("from_values");
			from_select = type.Equals("from_select");
			from_set = type.Equals("from_set");

			// ---

			// Check 'values_list' contains all same size size insert element arrays.
			int first_len = -1;
			for (int n = 0; n < values_list.Count; ++n) {
				IList exp_list = (IList)values_list[n];
				if (first_len == -1 || first_len == exp_list.Count) {
					first_len = exp_list.Count;
				} else {
					throw new DatabaseException("The insert data list varies in size.");
				}
			}

			tname = ResolveTableName(table_name, database);

			// Does the table exist?
			if (!database.TableExists(tname)) {
				throw new DatabaseException("Table '" + tname + "' does not exist.");
			}

			// Add the from table direct source for this table
			ITableQueryDef table_query_def = database.GetTableQueryDef(tname, null);
			AddTable(new FromTableDirectSource(database,
								   table_query_def, "INSERT_TABLE", tname, tname));

			// Get the table we are inserting to
			insert_table = database.GetTable(tname);

			// If column list is empty, then fill it with all columns from table.
			if (from_values || from_select) {
				// If 'col_list' is empty we must pick every entry from the insert
				// table.
				if (col_list.Count == 0) {
					for (int i = 0; i < insert_table.ColumnCount; ++i) {
						col_list.Add(insert_table.GetColumnDef(i).Name);
					}
				}
				// Resolve 'col_list' into a list of column indices into the insert
				// table.
				col_index_list = new int[col_list.Count];
				col_var_list = new Variable[col_list.Count];
				for (int i = 0; i < col_list.Count; ++i) {
					//        Variable col = Variable.resolve(tname, (String) col_list.get(i));
					Variable in_var = Variable.Resolve((String)col_list[i]);
					Variable col = ResolveColumn(in_var);
					int index = insert_table.FastFindFieldName(col);
					if (index == -1) {
						throw new DatabaseException("Can't find column: " + col);
					}
					col_index_list[i] = index;
					col_var_list[i] = col;
				}
			}

			// Make the 'from_values' clause into a 'from_set'
			if (from_values) {

				// If values to insert is different from columns list,
				if (col_list.Count != ((IList)values_list[0]).Count) {
					throw new DatabaseException("Number of columns to insert is " +
									 "different from columns selected to insert to.");
				}

				// Resolve all expressions in the added list.
				// For each value
				for (int i = 0; i < values_list.Count; ++i) {
					// Each value is a list of either expressions or "DEFAULT"
					IList insert_elements = (IList)values_list[i];
					int sz = insert_elements.Count;
					for (int n = 0; n < sz; ++n) {
						Object elem = insert_elements[n];
						if (elem is Expression) {
							Expression exp = (Expression)elem;
							IList elem_list = exp.AllElements;
							for (int p = 0; p < elem_list.Count; ++p) {
								Object ob = elem_list[p];
								if (ob is SelectStatement) {
									throw new DatabaseException(
													 "Illegal to have sub-select in expression.");
								}
							}
							// Resolve the expression.
							ResolveExpression(exp);
						}
					}
				}

			} else if (from_select) {
				// Prepare the select statement
				prepared_select = new SelectStatement();
				prepared_select.Init(database, select, null);
				prepared_select.Prepare();
			}

			// If from a set, then resolve all values,
			else if (from_set) {

				// If there's a sub select in an expression in the 'SET' clause then
				// throw an error.
				for (int i = 0; i < column_sets.Count; ++i) {
					Assignment assignment = (Assignment)column_sets[i];
					Expression exp = assignment.Expression;
					IList elem_list = exp.AllElements;
					for (int n = 0; n < elem_list.Count; ++n) {
						Object ob = elem_list[n];
						if (ob is SelectStatement) {
							throw new DatabaseException(
												 "Illegal to have sub-select in SET clause.");
						}
					}

					// Resolve the column names in the columns set.
					Variable v = assignment.Variable;
					Variable resolved_v = ResolveVariableName(v);
					v.Set(resolved_v);
					ResolveExpression(assignment.Expression);
				}

			}

			// Resolve all tables linked to this
			TableName[] linked_tables =
									 database.QueryTablesRelationallyLinkedTo(tname);
			relationally_linked_tables = new ArrayList(linked_tables.Length);
			for (int i = 0; i < linked_tables.Length; ++i) {
				relationally_linked_tables.Add(database.GetTable(linked_tables[i]));
			}

		}

		public override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(database);

			// Check that this user has privs to insert into the table.
			if (!database.Database.CanUserInsertIntoTableObject(
											context, user, tname, col_var_list)) {
				throw new UserAccessException(
				   "User not permitted to insert in to table: " + table_name);
			}

			// Are we inserting from a select statement or from a 'set' assignment
			// list?
			int insert_count = 0;

			if (from_values) {
				// Set each row from the VALUES table,
				for (int i = 0; i < values_list.Count; ++i) {
					IList insert_elements = (IList)values_list[i];
					RowData row_data = insert_table.createRowDataObject(context);
					row_data.SetupEntire(col_index_list, insert_elements, context);
					insert_table.Add(row_data);
					++insert_count;
				}
			} else if (from_select) {
				// Insert rows from the result select table.
				Table result = prepared_select.Evaluate();
				if (result.ColumnCount != col_index_list.Length) {
					throw new DatabaseException(
							"Number of columns in result don't match columns to insert.");
				}

				// Copy row list into an intermediate IntegerVector list.
				// (A IRowEnumerator for a table being modified is undefined).
				IntegerVector row_list = new IntegerVector();
				IRowEnumerator en = result.GetRowEnumerator();
				while (en.MoveNext()) {
					row_list.AddInt(en.RowIndex);
				}

				// For each row of the select table.
				int sz = row_list.Count;
				for (int i = 0; i < sz; ++i) {
					int rindex = row_list[i];
					RowData row_data = insert_table.createRowDataObject(context);
					for (int n = 0; n < col_index_list.Length; ++n) {
						TObject cell = result.GetCellContents(n, rindex);
						row_data.SetColumnData(col_index_list[n], cell);
					}
					row_data.SetToDefault(context);
					insert_table.Add(row_data);
					++insert_count;
				}
			} else if (from_set) {
				// Insert rows from the set assignments.
				RowData row_data = insert_table.createRowDataObject(context);
				Assignment[] assignments = (Assignment[])
							  column_sets.ToArray(typeof(Assignment));
				row_data.SetupEntire(assignments, context);
				insert_table.Add(row_data);
				++insert_count;
			}

			// Notify TriggerManager that we've just done an update.
			if (insert_count > 0) {
				database.OnTriggerEvent(new TriggerEvent(
								  TriggerEventType.Insert, tname.ToString(), insert_count));
			}

			// Return the number of rows we inserted.
			return FunctionTable.ResultTable(context, insert_count);
		}
	}
}