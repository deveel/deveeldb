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

namespace Deveel.Data.Query {
	/// <summary>
	/// A set of tables and function references that make up the resources 
	/// made available by a table expression.
	/// </summary>
	/// <remarks>
	/// When a <see cref="Sql.SelectStatement"/> is prepared this object is created 
	/// and is used to dereference names to sources. It also has the ability to 
	/// chain to another <see cref="TableExpressionFromSet"/> and resolve 
	/// references over a complex sub-query hierarchy.
	/// </remarks>
	public sealed class TableExpressionFromSet {
		/// <summary>
		/// The list of table resources in this set. (IFromTableSource).
		/// </summary>
		private readonly List<IFromTableSource> tableResources;

		/// <summary>
		/// The list of function expression resources.
		/// </summary>
		/// <example>
		/// For example, one table expression may expose a function as 
		/// <c>SELECT (a + b) AS c, ....</c> in which case we have a 
		/// virtual assignment of c = (a + b) in this set.
		/// </example>
		private readonly List<object> functionResources;

		/// <summary>
		/// The list of Variable references in this set that are exposed 
		/// to the outside, including function aliases.
		/// </summary>
		/// <example>
		/// For example, <c>SELECT a, b, c, (a + 1) d FROM ABCTable</c> 
		/// would be exposing variables 'a', 'b', 'c' and 'd'.
		/// </example>
		private readonly List<VariableName> exposedVariables;

		/// <summary>
		/// Set to true if this should do case insensitive resolutions.
		/// </summary>
		private bool caseInsensitive;

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
			tableResources = new List<IFromTableSource>();
			functionResources = new List<object>();
			exposedVariables = new List<VariableName>();
			// Is the database case insensitive?
			this.caseInsensitive = case_insensitive;
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
			caseInsensitive = status;
		}

		internal bool StringCompare(string str1, string str2) {
			return String.Compare(str1, str2, caseInsensitive) == 0;
		}

		/// <summary>
		/// Adds a table resource to the set.
		/// </summary>
		/// <param name="tableResource"></param>
		public void AddTable(IFromTableSource tableResource) {
			tableResources.Add(tableResource);
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
		public void AddFunctionRef(string name, Expression expression) {
			functionResources.Add(name);
			functionResources.Add(expression);
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
			exposedVariables.Add(v);
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
			if (table == null)
				throw new StatementException("Table name found: " + tn);

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
			int sz = exposedVariables.Count;
			VariableName[] list = new VariableName[sz];
			for (int i = 0; i < sz; ++i) {
				list[i] = new VariableName(exposedVariables[i]);
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
		internal IFromTableSource FindTable(string schema, string name) {
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
			get { return tableResources.Count; }
		}

		/// <summary>
		/// Returns the IFromTableSource object at the given index position in this set.
		/// </summary>
		/// <param name="i"></param>
		/// <returns></returns>
		internal IFromTableSource GetTable(int i) {
			return tableResources[i];
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
			string varName = v.Name;

			// We are guarenteed not to match with a function if the table name part
			// of a Variable is present.
			if (tname != null)
				return null;

			// Search for the function with this name
			Expression lastFound = null;
			int matchesFound = 0;
			for (int i = 0; i < functionResources.Count; i += 2) {
				string funName = (string)functionResources[i];
				if (StringCompare(funName, varName)) {
					if (matchesFound > 0)
						throw new StatementException("Ambiguous reference '" + v + "'");

					lastFound = (Expression)functionResources[i + 1];
					++matchesFound;
				}
			}

			return lastFound;
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
			string varName = v.Name;

			// We are guarenteed not to match with a function if the table name part
			// of a Variable is present.
			if (tname != null)
				return null;

			// Search for the function with this name
			VariableName lastFound = null;
			int matchesFound = 0;
			for (int i = 0; i < functionResources.Count; i+=2) {
				string funName = (string) functionResources[i];
				if (StringCompare(funName, varName)) {
					if (matchesFound > 0)
						throw new StatementException("Ambiguous reference '" + v + "'");

					lastFound = new VariableName(funName);
					++matchesFound;
				}
			}

			return lastFound;
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
			string schemaName = null;
			string tableName = null;
			string columnName = v.Name;
			if (tname != null) {
				schemaName = tname.Schema;
				tableName = tname.Name;
			}

			// Find matches in our list of tables sources,
			VariableName matchedVar = null;

			foreach (IFromTableSource table in tableResources) {
				int rcc = table.ResolveColumnCount(null, schemaName, tableName, columnName);
				if (rcc == 0) {
					// do nothing if no matches
				} else if (rcc == 1 && matchedVar == null) {
					// If 1 match and matched_var = null
					matchedVar = table.ResolveColumn(null, schemaName, tableName, columnName);
				} else {
#if DEBUG
					Console.Out.WriteLine(matchedVar);
					Console.Out.WriteLine(rcc);
#endif
					throw new StatementException("Ambiguous reference '" + v + "'");
				}
			}

			return matchedVar;
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
			List<VariableName> list = new List<VariableName>();

			VariableName functionVar = ResolveAssignmentReference(v);
			if (functionVar != null) {
				list.Add(functionVar);
			}

			VariableName tcVar = ResolveTableColumnReference(v);
			if (tcVar != null) {
				list.Add(tcVar);
			}

			// Return the variable if we found one unambiguously.
			int listSize = list.Count;
			if (listSize == 0)
				return null;
			if (listSize == 1)
				return list[0];

			throw new StatementException("Ambiguous reference '" + v + "'");
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
			if (nv == null && Parent != null)
				// If we need to descend to the parent, increment the level.
				return Parent.GlobalResolveReference(level + 1, v);
			if (nv != null)
				return new CorrelatedVariable(nv, level);
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
				throw new StatementException("Reference '" + v_in + "' not found.");
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