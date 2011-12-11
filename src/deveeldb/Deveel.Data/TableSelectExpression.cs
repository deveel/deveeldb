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

using Deveel.Data.Sql;

namespace Deveel.Data {
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
		private FromClause fromClause = new FromClause();

		/// <summary>
		/// The where clause.
		/// </summary>
		private SearchExpression whereClause;

		/// <summary>
		/// A flag indicating if the WHERE clause was set.
		/// </summary>
		private bool whereSet;

		/// <summary>
		/// The list of columns to group by.
		/// </summary>
		private List<ByColumn> groupBy = new List<ByColumn>();

		/// <summary>
		/// The group max variable or null if no group max.
		/// </summary>
		private VariableName groupMax;

		/// <summary>
		/// The having clause.
		/// </summary>
		private SearchExpression havingClause;

		private readonly SelectIntoClause into = new SelectIntoClause();


		/// <summary>
		/// If there is a composite function this is set to the composite 
		/// enumeration from CompositeTable.
		/// </summary>
		private CompositeFunction compositeFunction = CompositeFunction.None;  // (None)

		/// <summary>
		/// If this is an ALL composite (no removal of duplicate rows) it is true.
		/// </summary>
		private bool isCompositeAll;

		/// <summary>
		/// The composite table itself.
		/// </summary>
		private TableSelectExpression nextComposite;

		public TableSelectExpression() {
			whereClause = new SearchExpression(null);
			havingClause = new SearchExpression(null);
		}

		public FromClause From {
			get { return fromClause; }
		}

		public SelectIntoClause Into {
			get { return into; }
		}

		public SearchExpression Where {
			get { return whereClause; }
			set {
				whereClause = value;
				whereSet = true;
			}
		}

		public bool Distinct {
			get { return distinct; }
			set { distinct = value; }
		}

		public List<ByColumn> GroupBy {
			get { return groupBy; }
		}

		public VariableName GroupMax {
			get { return groupMax; }
			set { groupMax = value; }
		}

		public SearchExpression Having {
			get { return havingClause; }
			set { havingClause = value; }
		}

		public bool IsCompositeAll {
			get { return isCompositeAll; }
		}

		public CompositeFunction CompositeFunction {
			get { return compositeFunction; }
		}

		public TableSelectExpression NextComposite {
			get { return nextComposite; }
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
		///<param name="isAll"></param>
		/// <remarks>
		/// For example, if this expression is a <c>Union ALL</c> with 
		/// another expression it would be set through this method.
		/// </remarks>
		///<exception cref="ApplicationException"></exception>
		public void ChainComposite(TableSelectExpression expression, CompositeFunction composite, bool isAll) {
			nextComposite = expression;
			compositeFunction = composite;
			isCompositeAll = isAll;
		}

		// ---------- Implemented from IStatementTreeObject ----------

		/// <summary>
		/// Prepares all the expressions in the list.
		/// </summary>
		/// <param name="list"></param>
		/// <param name="preparer"></param>
		private static void PrepareAllInList(IList list, IExpressionPreparer preparer) {
			foreach (IStatementTreeObject ob in list) {
				ob.PrepareExpressions(preparer);
			}
		}


		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			PrepareAllInList(columns, preparer);
			if (fromClause != null)
				((IStatementTreeObject)fromClause).PrepareExpressions(preparer);
			if (whereClause != null)
				((IStatementTreeObject) whereClause).PrepareExpressions(preparer);

			PrepareAllInList(groupBy, preparer);

			if (havingClause != null)
				((IStatementTreeObject)havingClause).PrepareExpressions(preparer);

			// Go to the next chain
			if (nextComposite != null) {
				((IStatementTreeObject)nextComposite).PrepareExpressions(preparer);
			}
		}

		/// <inheritdoc/>
		public object Clone() {
			TableSelectExpression v = (TableSelectExpression)MemberwiseClone();
			if (columns != null)
				v.columns = (List<SelectColumn>)StatementTree.CloneSingleObject(columns);
			if (fromClause != null)
				v.fromClause = (FromClause)fromClause.Clone();
			if (whereClause != null)
				v.whereClause = (SearchExpression)whereClause.Clone();
			if (groupBy != null)
				v.groupBy = (List<ByColumn>)StatementTree.CloneSingleObject(groupBy);
			if (groupMax != null)
				v.groupMax = (VariableName)groupMax.Clone();
			if (havingClause != null)
				v.havingClause = (SearchExpression)havingClause.Clone();
			if (nextComposite != null)
				v.nextComposite = (TableSelectExpression)nextComposite.Clone();
			return v;
		}
	}
}