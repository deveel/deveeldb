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
using System.Text;

namespace Deveel.Data.Sql {
	/// <summary>
	/// A container object for the a table select expression
	/// </summary>
	/// <example>
	/// An example of the function of this container is:
	/// <code>
	///                SELECT [columns]
	///                  FROM [tables]
	///                 WHERE [search_clause]
	///              GROUP BY [column]
	///                HAVING [search_clause]
	///  [composite_function] [table_select_expression]
	/// </code>
	/// </example>
	/// <remarks>
	/// Note that a <see cref="TableSelectExpression"/> can be nested 
	/// in the various clauses of this object.
	/// </remarks>
	[Serializable]
	public sealed class TableSelectExpression : IStatementTreeObject {
		/// <summary>
		/// True if we only search for distinct elements.
		/// </summary>
		private bool distinct;

		/// <summary>
		/// The list of columns to select from. (SelectColumn)
		/// </summary>
		private List<SelectColumn> columns = new List<SelectColumn>();

		/// <summary>
		/// The from clause.
		/// </summary>
		private FromClause from_clause = new FromClause();

		/// <summary>
		/// The where clause.
		/// </summary>
		private SearchExpression where_clause = new SearchExpression();

		/// <summary>
		/// A flag indicating if the WHERE clause was set.
		/// </summary>
		private bool whereSet;

		/// <summary>
		/// The list of columns to group by.
		/// </summary>
		private List<ByColumn> group_by = new List<ByColumn>();

		/// <summary>
		/// The group max variable or null if no group max.
		/// </summary>
		private VariableName group_max;

		/// <summary>
		/// The having clause.
		/// </summary>
		private SearchExpression having_clause = new SearchExpression();

		private readonly IntoClause into = new IntoClause();


		/// <summary>
		/// If there is a composite function this is set to the composite 
		/// enumeration from CompositeTable.
		/// </summary>
		private CompositeFunction composite_function = CompositeFunction.None;  // (None)

		/// <summary>
		/// If this is an ALL composite (no removal of duplicate rows) it is true.
		/// </summary>
		private bool is_composite_all;

		/// <summary>
		/// The composite table itself.
		/// </summary>
		private TableSelectExpression next_composite;

		public FromClause From {
			get { return from_clause; }
		}

		public IntoClause Into {
			get { return into; }
		}

		public SearchExpression Where {
			get { return where_clause; }
			set {
				where_clause = value;
				whereSet = true;
			}
		}

		public bool Distinct {
			get { return distinct; }
			set { distinct = value; }
		}

		public List<ByColumn> GroupBy {
			get { return group_by; }
		}

		public VariableName GroupMax {
			get { return group_max; }
			set { group_max = value; }
		}

		public SearchExpression Having {
			get { return having_clause; }
			set { having_clause = value; }
		}

		public bool IsCompositeAll {
			get { return is_composite_all; }
		}

		public CompositeFunction CompositeFunction {
			get { return composite_function; }
		}

		public TableSelectExpression NextComposite {
			get { return next_composite; }
		}

		/// <summary>
		/// The list of columns to select from.
		/// </summary>
		public List<SelectColumn> Columns {
			get { return columns; }
		}

		///<summary>
		/// Chains a new composite function to this expression.
		///</summary>
		///<param name="expression"></param>
		///<param name="composite"></param>
		///<param name="is_all"></param>
		/// <remarks>
		/// For example, if this expression is a <c>Union ALL</c> with 
		/// another expression it would be set through this method.
		/// </remarks>
		///<exception cref="ApplicationException"></exception>
		public void ChainComposite(TableSelectExpression expression, CompositeFunction composite, bool is_all) {
			next_composite = expression;
			composite_function = composite;
			is_composite_all = is_all;
		}

		// ---------- Implemented from IStatementTreeObject ----------

		/// <summary>
		/// Prepares all the expressions in the list.
		/// </summary>
		/// <param name="list"></param>
		/// <param name="preparer"></param>
		private static void PrepareAllInList(IList list, IExpressionPreparer preparer) {
			for (int n = 0; n < list.Count; ++n) {
				IStatementTreeObject ob = (IStatementTreeObject)list[n];
				ob.PrepareExpressions(preparer);
			}
		}


		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			PrepareAllInList(columns, preparer);
			((IStatementTreeObject)from_clause).PrepareExpressions(preparer);
			((IStatementTreeObject) where_clause).PrepareExpressions(preparer);
			PrepareAllInList(group_by, preparer);
			((IStatementTreeObject)having_clause).PrepareExpressions(preparer);

			// Go to the next chain
			if (next_composite != null) {
				((IStatementTreeObject)next_composite).PrepareExpressions(preparer);
			}
		}

		/// <inheritdoc/>
		public Object Clone() {
			TableSelectExpression v = (TableSelectExpression)MemberwiseClone();
			if (columns != null)
				v.columns = (List<SelectColumn>)StatementTree.CloneSingleObject(columns);
			if (from_clause != null)
				v.from_clause = (FromClause)from_clause.Clone();
			if (where_clause != null)
				v.where_clause = (SearchExpression)where_clause.Clone();
			if (group_by != null)
				v.group_by = (List<ByColumn>)StatementTree.CloneSingleObject(group_by);
			if (group_max != null)
				v.group_max = (VariableName)group_max.Clone();
			if (having_clause != null)
				v.having_clause = (SearchExpression)having_clause.Clone();
			if (next_composite != null)
				v.next_composite = (TableSelectExpression)next_composite.Clone();
			return v;
		}

		public override string ToString() {
			StringBuilder sb = new StringBuilder("SELECT ");
			for (int i = 0; i < columns.Count; i++) {
				SelectColumn column = columns[i];
				column.DumpTo(sb);

				if (i < columns.Count - 1)
					sb.Append(", ");
			}

			sb.Append(" FROM ");
			from_clause.DumpTo(sb);

			if (whereSet) {
				sb.Append(" WHERE ");
				where_clause.DumpTo(sb);
			}

			return sb.ToString();
		}
	}
}