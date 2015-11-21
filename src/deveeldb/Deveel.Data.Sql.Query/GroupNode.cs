// 
//  Copyright 2010-2015 Deveel
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
//

using System;

using Deveel.Data;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Query {
	class GroupNode : SingleQueryPlanNode {
		public GroupNode(IQueryPlanNode child, ObjectName groupMaxColumn, SqlExpression[] functions, string[] names) 
			: this(child, new ObjectName[0], groupMaxColumn, functions, names) {
		}

		public GroupNode(IQueryPlanNode child, ObjectName[] columnNames, ObjectName groupMaxColumn, SqlExpression[] functions, string[] names) : base(child) {
			ColumnNames = columnNames;
			GroupMaxColumn = groupMaxColumn;
			Functions = functions;
			Names = names;
		}

		public ObjectName[] ColumnNames { get; private set; }

		public ObjectName GroupMaxColumn { get; private set; }

		public SqlExpression[] Functions { get; private set; }

		public string[] Names { get; private set; }

		public override ITable Evaluate(IQueryContext context) {
			var childTable = Child.Evaluate(context);
			var funTable = new FunctionTable(childTable, Functions, Names, context);

			// If no columns then it is implied the whole table is the group.
			if (ColumnNames == null) {
				funTable = funTable.AsGroup();
			} else {
				funTable = funTable.CreateGroupMatrix(ColumnNames);
			}

			return funTable.MergeWith(GroupMaxColumn);
		}
	}
}
