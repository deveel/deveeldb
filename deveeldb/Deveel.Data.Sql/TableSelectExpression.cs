//  
//  TableSelectExpression.cs
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
	public sealed class TableSelectExpression : IStatementTreeObject, ICloneable {
		/// <summary>
		/// True if we only search for distinct elements.
		/// </summary>
		private bool distinct;

		/// <summary>
		/// The list of columns to select from. (SelectColumn)
		/// </summary>
		private ArrayList columns = new ArrayList();

		/// <summary>
		/// The from clause.
		/// </summary>
		private FromClause from_clause = new FromClause();

		/// <summary>
		/// The where clause.
		/// </summary>
		private SearchExpression where_clause = new SearchExpression();


		/// <summary>
		/// The list of columns to group by. (ByColumn)
		/// </summary>
		private ArrayList group_by = new ArrayList();

		/// <summary>
		/// The group max variable or null if no group max.
		/// </summary>
		private VariableName group_max;

		/// <summary>
		/// The having clause.
		/// </summary>
		private SearchExpression having_clause = new SearchExpression();


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

		public SearchExpression Where {
			get { return where_clause; }
			set { where_clause = value; }
		}

		public bool Distinct {
			get { return distinct; }
			set { distinct = value; }
		}

		public IList GroupBy {
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
		/// The list of columns to select from. (SelectColumn)
		/// </summary>
		public ArrayList Columns {
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
			if (columns != null) {
				v.columns = (ArrayList)StatementTree.CloneSingleObject(columns);
			}
			if (from_clause != null) {
				v.from_clause = (FromClause)from_clause.Clone();
			}
			if (where_clause != null) {
				v.where_clause = (SearchExpression)where_clause.Clone();
			}
			if (group_by != null) {
				v.group_by = (ArrayList)StatementTree.CloneSingleObject(group_by);
			}
			if (group_max != null) {
				v.group_max = (VariableName)group_max.Clone();
			}
			if (having_clause != null) {
				v.having_clause = (SearchExpression)having_clause.Clone();
			}
			if (next_composite != null) {
				v.next_composite = (TableSelectExpression)next_composite.Clone();
			}
			return v;
		}
	}
}