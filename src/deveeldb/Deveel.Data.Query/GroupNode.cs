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
using System.Collections.Generic;
using System.Text;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Query {
	/// <summary>
	/// The node for performing a grouping operation on the columns of the 
	/// child node.
	/// </summary>
	/// <remarks>
	/// As well as grouping, any aggregate functions must also be defined
	/// with this plan.
	/// <para>
	/// <b>Note:</b> The whole child is a group if columns is null.
	/// </para>
	/// </remarks>
	[Serializable]
	public class GroupNode : SingleQueryPlanNode {
		/// <summary>
		/// The columns to group by.
		/// </summary>
		private readonly VariableName[] columns;

		/// <summary>
		/// The group max column.
		/// </summary>
		private VariableName groupMaxColumn;

		/// <summary>
		/// Any aggregate functions (or regular function columns) that 
		/// are to be planned.
		/// </summary>
		private readonly Expression[] functionList;

		/// <summary>
		/// The list of names to give each function table.
		/// </summary>
		private readonly String[] nameList;


		/// <summary>
		/// Groups over the given columns from the child.
		/// </summary>
		/// <param name="child"></param>
		/// <param name="columns"></param>
		/// <param name="groupMaxColumn"></param>
		/// <param name="functionList"></param>
		/// <param name="nameList"></param>
		public GroupNode(IQueryPlanNode child, VariableName[] columns, VariableName groupMaxColumn, Expression[] functionList, string[] nameList)
			: base(child) {
			this.columns = columns;
			this.groupMaxColumn = groupMaxColumn;
			this.functionList = functionList;
			this.nameList = nameList;
		}

		/// <summary>
		/// Groups over the entire child (always ends in 1 result in set).
		/// </summary>
		/// <param name="child"></param>
		/// <param name="groupMaxColumn"></param>
		/// <param name="functionList"></param>
		/// <param name="nameList"></param>
		public GroupNode(IQueryPlanNode child, VariableName groupMaxColumn, Expression[] functionList, String[] nameList)
			: this(child, null, groupMaxColumn, functionList, nameList) {
		}

		public override Table Evaluate(IQueryContext context) {
			Table child_table = Child.Evaluate(context);
			DatabaseQueryContext dbContext = (DatabaseQueryContext)context;
			FunctionTable funTable = new FunctionTable(child_table, functionList, nameList, dbContext);
			// If no columns then it is implied the whole table is the group.
			if (columns == null) {
				funTable.SetWholeTableAsGroup();
			} else {
				funTable.CreateGroupMatrix(columns);
			}
			return funTable.MergeWithReference(groupMaxColumn);
		}

		public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
			list = base.DiscoverTableNames(list);
			foreach (Expression expression in functionList) {
				list = expression.DiscoverTableNames(list);
			}
			return list;
		}

		public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
			list = base.DiscoverCorrelatedVariables(level, list);
			foreach (Expression expression in functionList) {
				list = expression.DiscoverCorrelatedVariables(ref level, list);
			}
			return list;
		}

		public override Object Clone() {
			GroupNode node = (GroupNode)base.Clone();
			QueryPlanUtil.CloneArray(node.columns);
			QueryPlanUtil.CloneArray(node.functionList);
			if (groupMaxColumn != null) {
				node.groupMaxColumn = (VariableName)groupMaxColumn.Clone();
			} else {
				node.groupMaxColumn = null;
			}
			return node;
		}

		public override string Title {
			get {
				StringBuilder sb = new StringBuilder();
				sb.Append("GROUP: (");
				if (columns == null) {
					sb.Append("WHOLE TABLE");
				} else {
					for (int i = 0; i < columns.Length; ++i) {
						sb.Append(columns[i]);
						if (i < columns.Length - 1)
							sb.Append(", ");
					}
				}
				sb.Append(")");
				if (functionList != null) {
					sb.Append(" FUNS: [");
					for (int i = 0; i < functionList.Length; ++i) {
						sb.Append(functionList[i]);
						if (i < functionList.Length - 1)
							sb.Append(", ");
					}
					sb.Append("]");
				}
				return sb.ToString();
			}
		}
	}
}