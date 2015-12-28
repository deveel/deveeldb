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
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Parser {
	class CreateTableNode : SqlStatementNode {
		public ObjectNameNode TableName { get; private set; }

		public bool IfNotExists { get; private set; }

		public bool Temporary { get; private set; }

		public IEnumerable<TableColumnNode> Columns { get; private set; }

		public IEnumerable<TableConstraintNode> Constraints { get; private set; }

		protected override void OnNodeInit() {
			TableName = this.FindNode<ObjectNameNode>();
			IfNotExists = this.HasOptionalNode("if_not_exists_opt");
			Temporary = this.HasOptionalNode("temporary_opt");

			var elements = this.FindNodes<ITableElementNode>().ToList();
			Columns = elements.OfType<TableColumnNode>();
			Constraints = elements.OfType<TableConstraintNode>();
		}

		protected override void BuildStatement(SqlCodeObjectBuilder builder) {
			Build(builder.TypeResolver, builder);
		}

		public void Build(ITypeResolver typeResolver, SqlCodeObjectBuilder builder) {
			string idColumn = null;

			var tableName = TableName;
			var constraints = new List<SqlTableConstraint>();
			var columns = new List<SqlTableColumn>();

			foreach (var column in Columns) {
				if (column.IsIdentity) {
					if (!String.IsNullOrEmpty(idColumn))
						throw new InvalidOperationException(String.Format("Table {0} defines already {1} as identity column.", TableName,
							idColumn));

					if (column.Default != null)
						throw new InvalidOperationException(String.Format("The identity column {0} cannot have a DEFAULT constraint.",
							idColumn));

					idColumn = column.ColumnName;
				}

				var columnInfo = column.BuildColumn(typeResolver, tableName.Name, constraints);

				columns.Add(columnInfo);
			}

			//TODO: Optimization: merge same constraints

			builder.AddObject(MakeCreateTable(tableName.Name, columns, IfNotExists, Temporary));

			foreach (var constraint in Constraints) {
				var constraintInfo = constraint.BuildConstraint();
				builder.AddObject(new AlterTableStatement(ObjectName.Parse(tableName.Name), new AddConstraintAction(constraintInfo)));
			}

			foreach (var constraint in constraints) {
				builder.AddObject(MakeAlterTableAddConstraint(tableName.Name, constraint));
			}
		}

		private static SqlStatement MakeAlterTableAddConstraint(string tableName, SqlTableConstraint constraint) {
			var action = new AddConstraintAction(constraint);

			return new AlterTableStatement(ObjectName.Parse(tableName), action);
		}

		private static SqlStatement MakeCreateTable(string tableName, IEnumerable<SqlTableColumn> columns, bool ifNotExists,
			bool temporary) {
			var objTableName = ObjectName.Parse(tableName);
			var tree = new CreateTableStatement(objTableName, columns.ToList());
			tree.IfNotExists = ifNotExists;
			tree.Temporary = temporary;
			return tree;
		}
	}
}