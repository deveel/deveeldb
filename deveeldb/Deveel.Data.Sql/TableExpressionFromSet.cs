//  
//  TableExpressionFromSet.cs
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
	/// A set of tables and function references that make up the resources 
	/// made available by a table expression.
	/// </summary>
	/// <remarks>
	/// When a <see cref="SelectStatement"/> is prepared this object is created 
	/// and is used to dereference names to sources. It also has the ability to 
	/// chain to another <see cref="TableExpressionFromSet"/> and resolve 
	/// references over a complex sub-query hierarchy.
	/// </remarks>
	class TableExpressionFromSet {
		/// <summary>
		/// The list of table resources in this set. (IFromTableSource).
		/// </summary>
		private readonly ArrayList table_resources;

		/// <summary>
		/// The list of function expression resources.
		/// </summary>
		/// <example>
		/// For example, one table expression may expose a function as 
		/// <c>SELECT (a + b) AS c, ....</c> in which case we have a 
		/// virtual assignment of c = (a + b) in this set.
		/// </example>
		private readonly ArrayList function_resources;

		/// <summary>
		/// The list of Variable references in this set that are exposed 
		/// to the outside, including function aliases.
		/// </summary>
		/// <example>
		/// For example, <c>SELECT a, b, c, (a + 1) d FROM ABCTable</c> 
		/// would be exposing variables 'a', 'b', 'c' and 'd'.
		/// </example>
		private readonly ArrayList exposed_variables;

		/// <summary>
		/// Set to true if this should do case insensitive resolutions.
		/// </summary>
		private bool case_insensitive = false;

		/// <summary>
		/// The parent TableExpressionFromSet if one exists.
		/// </summary>
		/// <remarks>
		/// This is used for chaining a set of table sets together. 
		/// When chained the <see cref="GlobalResolveReference"/> 
		/// method can be used to resolve a reference in the chain.
		/// </remarks>
		private TableExpressionFromSet parent;


		/// <summary>
		/// 
		/// </summary>
		/// <param name="case_insensitive"></param>
		public TableExpressionFromSet(bool case_insensitive) {
			table_resources = new ArrayList();
			function_resources = new ArrayList();
			exposed_variables = new ArrayList();
			// Is the database case insensitive?
			this.case_insensitive = case_insensitive;
		}

		/// <summary>
		/// Gets or sets the parent expression for the current one.
		/// </summary>
		/// <remarks>
		/// Thi can be setted or returns <b>null</b> if the expression
		/// has no parent.
		/// </remarks>
		public TableExpressionFromSet Parent {
			get { return parent; }
			set { parent = value; }
		}

		/// <summary>
		/// Toggle the case sensitivity flag.
		/// </summary>
		/// <param name="status"></param>
		public void SetCaseInsensitive(bool status) {
			case_insensitive = status;
		}

		internal bool StringCompare(String str1, String str2) {
			if (!case_insensitive) {
				return str1.Equals(str2);
			}
			return String.Compare(str1, str2, true) == 0;
		}

		/// <summary>
		/// Adds a table resource to the set.
		/// </summary>
		/// <param name="table_resource"></param>
		public void AddTable(IFromTableSource table_resource) {
			table_resources.Add(table_resource);
		}

		/// <summary>
		/// Adds a function resource to the set.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="expression"></param>
		/// <remarks>
		/// Note that is possible for there to be references in the 
		/// <paramref name="expression"/> that do not reference resources 
		/// in this set (eg. a correlated reference).
		/// </remarks>
		public void AddFunctionRef(String name, Expression expression) {
			//    Console.Out.WriteLine("AddFunctionRef: " + name + ", " + expression);
			function_resources.Add(name);
			function_resources.Add(expression);
		}

		/// <summary>
		/// Adds a variable in this from set that is exposed to the outside.
		/// </summary>
		/// <param name="v"></param>
		/// <remarks>
		/// This list should contain all references from the <c>SELECT ...</c>
		/// part of the query (eg. <c>SELECT a, b, (a + 1) d</c> exposes 
		/// variables a, b and d).
		/// </remarks>
		public void ExposeVariable(VariableName v) {
			//    Console.Out.WriteLine("ExposeVariable: " + v);
			//    Console.Out.WriteLine(new Exception().StackTrace);
			exposed_variables.Add(v);
		}

		/// <summary>
		/// Exposes all the columns from the given <see cref="IFromTableSource"/>.
		/// </summary>
		/// <param name="table"></param>
		public void ExposeAllColumnsFromSource(IFromTableSource table) {
			VariableName[] v = table.AllColumns;
			for (int p = 0; p < v.Length; ++p) {
				ExposeVariable(v[p]);
			}
		}

		/// <summary>
		/// Exposes all the columns in all the child tables.
		/// </summary>
		public void ExposeAllColumns() {
			for (int i = 0; i < SetCount; ++i) {
				ExposeAllColumnsFromSource(GetTable(i));
			}
		}

		/// <summary>
		/// Exposes all the columns from the given table name.
		/// </summary>
		/// <param name="tn"></param>
		public void ExposeAllColumnsFromSource(TableName tn) {
			IFromTableSource table = FindTable(tn.Schema, tn.Name);
			if (table == null) {
				throw new StatementException("Table name found: " + tn);
			}
			ExposeAllColumnsFromSource(table);
		}

		/// <summary>
		/// Gets all the variables exposed in the set.
		/// </summary>
		/// <remarks>
		/// This is a list of fully qualified variables that are
		/// referencable from the final result of the table expression.
		/// </remarks>
		/// <returns>
		/// Returns an array of <see cref="VariableName"/> representing all the 
		/// variables exposed in the set.
		/// </returns>
		public VariableName[] GenerateResolvedVariableList() {
			int sz = exposed_variables.Count;
			VariableName[] list = new VariableName[sz];
			for (int i = 0; i < sz; ++i) {
				list[i] = new VariableName((VariableName)exposed_variables[i]);
			}
			return list;
		}

		/// <summary>
		/// Finds the first table for the given schema and name.
		/// </summary>
		/// <param name="schema"></param>
		/// <param name="name"></param>
		/// <returns>
		/// Returns the first <see cref="IFromTableSource"/> that matches the given 
		/// <paramref name="schema"/> and <paramref name="name"/> reference 
		/// or <b>null</b> if no objects with the given <paramref name="schema"/> 
		/// and <paramref name="name"/> reference match.
		/// </returns>
		internal IFromTableSource FindTable(String schema, String name) {
			for (int p = 0; p < SetCount; ++p) {
				IFromTableSource table = GetTable(p);
				if (table.MatchesReference(null, schema, name)) {
					return table;
				}
			}
			return null;
		}

		/// <summary>
		/// Returns the number of IFromTableSource objects in this set.
		/// </summary>
		internal int SetCount {
			get { return table_resources.Count; }
		}

		/// <summary>
		/// Returns the IFromTableSource object at the given index position in this set.
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		internal IFromTableSource GetTable(int i) {
			return (IFromTableSource)table_resources[i];
		}


		/// <summary>
		/// Dereferences a fully qualified reference that is within the 
		/// set.
		/// </summary>
		/// <param name="v"></param>
		/// <example>
		/// For example, <c>SELECT ( a + b ) AS z</c> given <i>z</i> would 
		/// return the expression <c>(a + b)</c>.
		/// </example>
		/// <returns>
		/// Returns the expression part of the assignment or <b>null</b>
		/// if unable to dereference assignment because it does not
		/// exist.
		/// </returns>
		internal Expression DereferenceAssignment(VariableName v) {
			TableName tname = v.TableName;
			String var_name = v.Name;
			// We are guarenteed not to match with a function if the table name part
			// of a Variable is present.
			if (tname != null) {
				return null;
			}

			// Search for the function with this name
			Expression last_found = null;
			int matches_found = 0;
			for (int i = 0; i < function_resources.Count; i += 2) {
				String fun_name = (String)function_resources[i];
				if (StringCompare(fun_name, var_name)) {
					if (matches_found > 0) {
						throw new StatementException("Ambiguous reference '" + v + "'");
					}
					last_found = (Expression)function_resources[i + 1];
					++matches_found;
				}
			}

			return last_found;
		}

		/// <summary>
		/// Resolves the given Variable object to an assignment if it's possible 
		/// to do so within the context of this set.
		/// </summary>
		/// <param name="v"></param>
		/// <remarks>
		/// If the variable isn't assigned to any function or aliased column, 'null' is returned.
		/// </remarks>
		/// <returns></returns>
		/// <exception cref="StatementException">
		/// If the variable can not be unambiguously resolved to a function or aliased 
		/// column.
		/// </exception>
		private VariableName ResolveAssignmentReference(VariableName v) {
			TableName tname = v.TableName;
			String var_name = v.Name;
			// We are guarenteed not to match with a function if the table name part
			// of a Variable is present.
			if (tname != null) {
				return null;
			}

			// Search for the function with this name
			VariableName last_found = null;
			int matches_found = 0;
			for (int i = 0; i < function_resources.Count; i += 2) {
				String fun_name = (String)function_resources[i];
				if (StringCompare(fun_name, var_name)) {
					if (matches_found > 0) {
						throw new StatementException("Ambiguous reference '" + v + "'");
					}
					last_found = new VariableName(fun_name);
					++matches_found;
				}
			}

			return last_found;
		}


		/// <summary>
		/// Resolves the given variable against the table columns in the 
		/// from set.
		/// </summary>
		/// <param name="v"></param>
		/// <remarks>
		/// Note that the given variable does not have to be fully qualified 
		/// but the returned expressions are fully qualified.
		/// </remarks>
		/// <returns>
		/// Returns a resolved <see cref="VariableName"/> or <b>nnull</b>, if the 
		/// variable does not resolve to anything.
		/// </returns>
		/// <exception cref="StatementException">
		/// If the variable is an ambiguous reference.
		/// </exception>
		private VariableName ResolveTableColumnReference(VariableName v) {
			TableName tname = v.TableName;
			String sch_name = null;
			String tab_name = null;
			String col_name = v.Name;
			if (tname != null) {
				sch_name = tname.Schema;
				tab_name = tname.Name;
			}

			// Find matches in our list of tables sources,
			VariableName matched_var = null;

			for (int i = 0; i < table_resources.Count; ++i) {
				IFromTableSource table = (IFromTableSource)table_resources[i];
				int rcc = table.ResolveColumnCount(null, sch_name, tab_name, col_name);
				if (rcc == 0) {
					// do nothing if no matches
				} else if (rcc == 1 && matched_var == null) {
					// If 1 match and matched_var = null
					matched_var =
								 table.ResolveColumn(null, sch_name, tab_name, col_name);
				} else {  // if (rcc >= 1 and matched_var != null)
					Console.Out.WriteLine(matched_var);
					Console.Out.WriteLine(rcc);
					throw new StatementException("Ambiguous reference '" + v + "'");
				}
			}

			return matched_var;
		}

		/// <summary>
		/// Resolves the given variable to a fully resolved variable
		/// within the context of the table expression.
		/// </summary>
		/// <param name="v"></param>
		/// <remarks>
		/// If the variable name references a table column, an expression 
		/// with a single <see cref="VariableName"/> element is returned.
		/// If the variable name references a function, an expression of the 
		/// function is returned.
		/// <para>
		/// Note that the given variable does not have to be fully qualified 
		/// but the returned expressions are fully qualified.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns a resolved <see cref="VariableName"/> or <b>nnull</b>,
		/// if the variable does not resolve to anything.
		/// </returns>
		/// <exception cref="StatementException">
		/// If the variable is an ambiguous reference.
		/// </exception>
		internal VariableName ResolveReference(VariableName v) {
			// Try and resolve against alias names first,
			ArrayList list = new ArrayList();

			//    Expression exp = DereferenceAssignment(v);
			//    // If this is an alias like 'a AS b' then add 'a' to the list instead of
			//    // adding 'b'.  This allows us to handle a number of ambiguous conditions.
			//    if (exp != null) {
			//      Variable v2 = exp.getVariable();
			//      if (v2 != null) {
			//        list.add(ResolveTableColumnReference(v2));
			//      }
			//      else {
			//        list.add(ResolveAssignmentReference(v));
			//      }
			//    }

			VariableName function_var = ResolveAssignmentReference(v);
			if (function_var != null) {
				list.Add(function_var);
			}

			VariableName tc_var = ResolveTableColumnReference(v);
			if (tc_var != null) {
				list.Add(tc_var);
			}

			//    TableName tname = v.GetTableName();
			//    String sch_name = null;
			//    String tab_name = null;
			//    String col_name = v.getName();
			//    if (tname != null) {
			//      sch_name = tname.getSchema();
			//      tab_name = tname.getName();
			//    }
			//
			//    // Find matches in our list of tables sources,
			//    for (int i = 0; i < table_resources.size(); ++i) {
			//      IFromTableSource table = (IFromTableSource) table_resources.get(i);
			//      int rcc = table.ResolveColumnCount(null, sch_name, tab_name, col_name);
			//      if (rcc == 1) {
			//        Variable matched =
			//                      table.ResolveColumn(null, sch_name, tab_name, col_name);
			//        list.add(matched);
			//      }
			//      else if (rcc > 1) {
			//        throw new StatementException("Ambiguous reference '" + v + "'");
			//      }
			//    }

			// Return the variable if we found one unambiguously.
			int list_size = list.Count;
			if (list_size == 0) {
				return null;
			} else if (list_size == 1) {
				return (VariableName)list[0];
			} else {
				//      // Check if the variables are the same?
				//      Variable cv = (Variable) list.get(0);
				//      for (int i = 1; i < list.size(); ++i) {
				//        if (!cv.equals(list.get(i))) {
				throw new StatementException("Ambiguous reference '" + v + "'");
				//        }
				//      }
				//      // If they are all the same return the variable.
				//      return v;
			}

		}

		/// <summary>
		/// Resolves the given variable reference within the chained list 
		/// of <see cref="TableExpressionFromSet"/> to a correlated variable.
		/// </summary>
		/// <param name="level"></param>
		/// <param name="v"></param>
		/// <remarks>
		/// If the reference is not found in this set the method recurses 
		/// to the parent set.
		/// </remarks>
		/// <returns>
		/// Returns the first unanbigous reference as a <see cref="CorrelatedVariable"/> 
		/// or <b>null</b> if the reference could not be resolved.
		/// </returns>
		/// <exception cref="StatementException">
		/// If resolution is ambiguous within a set.
		/// </exception>
		private CorrelatedVariable GlobalResolveReference(int level, VariableName v) {
			VariableName nv = ResolveReference(v);
			if (nv == null && Parent != null) {
				// If we need to descend to the parent, increment the level.
				return Parent.GlobalResolveReference(level + 1, v);
			} else if (nv != null) {
				return new CorrelatedVariable(nv, level);
			}
			return null;
		}

		/// <summary>
		/// Attempts to qualify the given Variable object to a value found 
		/// either in the current from set, or a value in the parent 
		/// from set.
		/// </summary>
		/// <param name="v_in"></param>
		/// <remarks>
		/// A variable that is qualified by the parent is called a 
		/// correlated variable.
		/// </remarks>
		/// <returns></returns>
		private Object QualifyVariable(VariableName v_in) {
			VariableName v = ResolveReference(v_in);
			if (v == null) {
				// If not found, try and resolve in parent set (correlated)
				if (Parent != null) {
					CorrelatedVariable cv = Parent.GlobalResolveReference(1, v_in);
					if (cv == null) {
						throw new StatementException("Reference '" +
													 v_in + "' not found.");
					}
					return cv;
				}
				//TODO: check this...
				if (v == null) {
					throw new StatementException("Reference '" +
												 v_in + "' not found.");
				}
			}
			return v;
		}

		/// <summary>
		/// Returns an <see cref="IExpressionPreparer"/> that qualifies all 
		/// variables in an expression to either a qualified <see cref="VariableName"/> 
		/// or a <see cref="CorrelatedVariable"/>.
		/// </summary>
		internal IExpressionPreparer ExpressionQualifier {
			get { return new ExpressionPreparerImpl(this); }
		}

		private class ExpressionPreparerImpl : IExpressionPreparer {
			public ExpressionPreparerImpl(TableExpressionFromSet expression) {
				this.expression = expression;
			}

			private readonly TableExpressionFromSet expression;

			public bool CanPrepare(Object element) {
				return element is VariableName;
			}
			public Object Prepare(Object element) {
				return expression.QualifyVariable((VariableName)element);
			}
		}
	}
}