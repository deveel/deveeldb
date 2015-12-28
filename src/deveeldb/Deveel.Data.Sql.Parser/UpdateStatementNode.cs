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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parser {
	class UpdateStatementNode : SqlStatementNode {
		public SimpleUpdateNode SimpleUpdate { get; private set; }

		public QueryUpdateNode QueryUpdate { get; private set; }

		protected override ISqlNode OnChildNode(ISqlNode node) {
			if (node is SimpleUpdateNode) {
				SimpleUpdate = (SimpleUpdateNode) node;
			} else if (node is QueryUpdateNode) {
				QueryUpdate = (QueryUpdateNode) node;
			}

			return base.OnChildNode(node);
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			if (SimpleUpdate != null) {
				BuildSimpleUpdate(builder, SimpleUpdate);
			} else if (QueryUpdate != null) {
				BuildQueryUpdate(builder, QueryUpdate);
			}
		}

		private void BuildSimpleUpdate(SqlCodeObjectBuilder builder, SimpleUpdateNode node) {
			var whereExpression = ExpressionBuilder.Build(node.WhereExpression);
			var assignments = UpdateAssignments(node.Columns);
			var statement = new UpdateStatement(node.TableName, whereExpression, assignments);
			statement.Limit = node.Limit;
			builder.AddObject(statement);
		}

		private IEnumerable<SqlColumnAssignment> UpdateAssignments(IEnumerable<UpdateColumnNode> columns) {
			if (columns == null)
				return null;

			return columns.Select(column => new SqlColumnAssignment(column.ColumnName, ExpressionBuilder.Build(column.Expression)));
		}

		private void BuildQueryUpdate(SqlCodeObjectBuilder builder, QueryUpdateNode node) {
			throw new NotImplementedException();
		}
	}
}