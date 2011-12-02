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

using Deveel.Data.Collections;

namespace Deveel.Data.Sql {
	/// <summary>
	/// The instance class that stores all the information about an 
	/// insert statement for processing.
	/// </summary>
	public class InsertStatement : Statement {
		private string table_name;
		private IList col_list;
		private IList values_list;    //list contains List of elements to insert
		private StatementTree select;
		private IList column_sets;

		private bool from_values = false;
		private bool from_select = false;
		private bool from_set = false;

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
		private VariableName[] col_var_list;

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

		protected override void Prepare() {

			// Prepare this object from the StatementTree
			table_name = GetString("table_name");
			col_list = GetList("col_list");
			values_list = GetList("data_list");
			select = (StatementTree)GetValue("select");
			column_sets = GetList("assignments");
			String type = GetString("type");
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

			tname = ResolveTableName(table_name, Connection);

			// Does the table exist?
			if (!Connection.TableExists(tname)) {
				throw new DatabaseException("Table '" + tname + "' does not exist.");
			}

			// Add the from table direct source for this table
			ITableQueryDef table_query_def = Connection.GetTableQueryDef(tname, null);
			AddTable(new FromTableDirectSource(Connection.IsInCaseInsensitiveMode, table_query_def, "INSERT_TABLE", tname, tname));

			// Get the table we are inserting to
			insert_table = Connection.GetTable(tname);

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
				col_var_list = new VariableName[col_list.Count];
				for (int i = 0; i < col_list.Count; ++i) {
					//        Variable col = Variable.resolve(tname, (String) col_list.get(i));
					VariableName in_var = VariableName.Resolve((String)col_list[i]);
					VariableName col = ResolveColumn(in_var);
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
									throw new DatabaseException("Illegal to have sub-select in expression.");
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
				prepared_select.Init(Connection, select, null);
				prepared_select.PrepareStatement();
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
						object ob = elem_list[n];
						if (ob is SelectStatement) {
							throw new DatabaseException("Illegal to have sub-select in SET clause.");
						}
					}

					// Resolve the column names in the columns set.
					VariableName v = assignment.VariableName;
					VariableName resolved_v = ResolveVariableName(v);
					v.Set(resolved_v);
					ResolveExpression(assignment.Expression);
				}

			}

			// Resolve all tables linked to this
			TableName[] linked_tables =
									 Connection.QueryTablesRelationallyLinkedTo(tname);
			relationally_linked_tables = new ArrayList(linked_tables.Length);
			for (int i = 0; i < linked_tables.Length; ++i) {
				relationally_linked_tables.Add(Connection.GetTable(linked_tables[i]));
			}

		}

		protected override Table Evaluate() {

			DatabaseQueryContext context = new DatabaseQueryContext(Connection);

			// Check that this user has privs to insert into the table.
			if (!Connection.Database.CanUserInsertIntoTableObject(context, User, tname, col_var_list))
				throw new UserAccessException("User not permitted to insert in to table: " + table_name);

			// Are we inserting from a select statement or from a 'set' assignment
			// list?
			int insert_count = 0;

			if (from_values) {
				// Set each row from the VALUES table,
				for (int i = 0; i < values_list.Count; ++i) {
					IList insert_elements = (IList)values_list[i];
					DataRow dataRow = insert_table.NewRow();
					dataRow.SetupEntire(col_index_list, insert_elements, context);
					insert_table.Add(dataRow);
					++insert_count;
				}
			} else if (from_select) {
				// Insert rows from the result select table.
				Table result = prepared_select.EvaluateStatement();
				if (result.ColumnCount != col_index_list.Length) {
					throw new DatabaseException(
							"Number of columns in result don't match columns to insert.");
				}

				// Copy row list into an intermediate IntegerVector list.
				// (A IRowEnumerator for a table being modified is undefined).
				IntegerVector row_list = new IntegerVector();
				IRowEnumerator en = result.GetRowEnumerator();
				while (en.MoveNext())
					row_list.AddInt(en.RowIndex);

				// For each row of the select table.
				int sz = row_list.Count;
				for (int i = 0; i < sz; ++i) {
					int rindex = row_list[i];
					DataRow dataRow = insert_table.NewRow();
					for (int n = 0; n < col_index_list.Length; ++n) {
						TObject cell = result.GetCellContents(n, rindex);
						dataRow.SetValue(col_index_list[n], cell);
					}
					dataRow.SetToDefault(context);
					insert_table.Add(dataRow);
					++insert_count;
				}
			} else if (from_set) {
				// Insert rows from the set assignments.
				DataRow dataRow = insert_table.NewRow();
				Assignment[] assignments = new Assignment[column_sets.Count];
				column_sets.CopyTo(assignments, 0);
				dataRow.SetupEntire(assignments, context);
				insert_table.Add(dataRow);
				++insert_count;
			}

			// Notify TriggerManager that we've just done an update.
			if (insert_count > 0)
				Connection.OnTriggerEvent(new TriggerEvent(TriggerEventType.Insert, tname.ToString(), insert_count));

			// Return the number of rows we inserted.
			return FunctionTable.ResultTable(context, insert_count);
		}
	}
}