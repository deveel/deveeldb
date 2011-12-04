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

namespace Deveel.Data.QueryPlanning {
	/// <summary>
	/// The node for performing an equi-select on a group of columns of the child node.
	/// </summary>
	/// <remarks>
	/// This is a separate node instead of chained IndexedSelectNode's so that we 
	/// might exploit multi-column indexes.
	/// </remarks>
	[Serializable]
	public class MultiColumnEquiSelectNode : SingleQueryPlanNode {
		/// <summary>
		/// The list of columns to select the range of.
		/// </summary>
		private readonly VariableName[] columns;

		/// <summary>
		/// The values of the cells to equi-select (must be constant expressions).
		/// </summary>
		private readonly Expression[] values;

		public MultiColumnEquiSelectNode(IQueryPlanNode child, VariableName[] columns, Expression[] values)
			: base(child) {
			this.columns = columns;
			this.values = values;
		}

		public override Table Evaluate(IQueryContext context) {
			Table t = Child.Evaluate(context);

			// TODO: Exploit multi-column indexes when they are implemented...

			// We select each column in turn
			Operator equalsOp = Operator.Equal;
			for (int i = 0; i < columns.Length; ++i) {
				t = t.SimpleSelect(context, columns[i], equalsOp, values[i]);
			}

			return t;
		}

		public override IList<TableName> DiscoverTableNames(IList<TableName> list) {
			throw new NotImplementedException();
		}

		public override IList<CorrelatedVariable> DiscoverCorrelatedVariables(int level, IList<CorrelatedVariable> list) {
			throw new NotImplementedException();
		}

		public override Object Clone() {
			MultiColumnEquiSelectNode node = (MultiColumnEquiSelectNode)base.Clone();
			QueryPlanUtil.CloneArray(node.columns);
			QueryPlanUtil.CloneArray(node.values);
			return node;
		}
	}
}