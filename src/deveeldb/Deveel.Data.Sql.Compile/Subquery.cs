// 
//  Copyright 2010-2016 Deveel
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
using System.Linq;

using Antlr4.Runtime.Misc;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Compile {
	static class Subquery {
		public static SqlQueryExpression Form(PlSqlParser.SubqueryContext context) {
			IntoClause into;
			return Form(context, out into);
		}

		public static SqlQueryExpression Form(PlSqlParser.SubqueryContext context, out IntoClause into) {
			var query = Form(context.subqueryBasicElements(), out into);

			var opPart = context.subquery_operation_part();

			if (opPart.Length > 0) {
				if (into != null)
					throw new InvalidOperationException("Cannot SELECT INTO in a composite query.");

				foreach (var part in opPart) {
					CompositeFunction function;
					if (part.MINUS() != null || part.EXCEPT() != null) {
						function = CompositeFunction.Except;
					} else if (part.UNION() != null) {
						function = CompositeFunction.Union;
					} else if (part.INTERSECT() != null) {
						function = CompositeFunction.Intersect;
					} else {
						throw new ParseCanceledException("Invalid composite function.");
					}

					bool isAll = part.ALL() != null;

					var next = Form(part.subqueryBasicElements());
					var prev = query.NextComposite;

					if (prev == null) {
						prev = next;
					} else {
						prev.NextComposite = next;
					}

					query.IsCompositeAll = isAll;
					query.CompositeFunction = function;
					query.NextComposite = prev;
				}
			}

			return query;
		}

		private static SqlQueryExpression Form(PlSqlParser.SubqueryBasicElementsContext context) {
			IntoClause into;
			return Form(context, out into);
		}

		private static SqlQueryExpression Form(PlSqlParser.SubqueryBasicElementsContext context, out IntoClause into) {
			var sub = context.subquery();
			if (sub != null && !sub.IsEmpty)
				return Form(sub, out into);

			return Form(context.queryBlock(), out into);
		}

		private static SqlQueryExpression Form(PlSqlParser.QueryBlockContext context, out IntoClause into) {
			var fromClause = FromClauseBuilder.Build(context.fromClause());

			SelectColumn[] columns;

			if (context.all != null) {
				columns = new[] {new SelectColumn(SqlExpression.Reference(new ObjectName("*")))};
			} else {
				columns = context.selectedElement().Select(SelectElement.BuildColumn).ToArray();
			}

			var query = new SqlQueryExpression(columns);

			into = null;

			if (context.DISTINCT() != null ||
				context.UNIQUE() != null)
				query.Distinct = true;

			var intoClause = context.into_clause();
			if (intoClause != null) {
				into = new IntoClause();

				if (intoClause.objectName() != null) {
					into.TableName = Name.Object(intoClause.objectName());
				} else if (intoClause.variable_name() != null) {
					into.Variables = intoClause.variable_name().Select(Name.Variable).ToArray();
				}
			}

			if (fromClause != null)
				query.FromClause = fromClause;

			var groupBy = context.groupByClause();
			if (groupBy != null && !groupBy.IsEmpty) {
				query.GroupBy = groupBy.groupByElements().expression().Select(x => new SqlExpressionVisitor().Visit(x));

				var having = groupBy.havingClause();
				if (having != null)
					query.HavingExpression = new SqlExpressionVisitor().Visit(having.condition());
			}

			var groupMax = context.groupMaxClause();
			if (groupMax != null && !groupMax.IsEmpty) {
				var maxColumn = Name.Object(groupMax.objectName());
				query.GroupMax = maxColumn;
			}

			var whereClause = context.whereClause();
			if (whereClause != null && !whereClause.IsEmpty) {
				var currentOf = whereClause.current_of_clause();
				if (currentOf != null && !currentOf.IsEmpty) {
					var cursorName = Name.Simple(currentOf.cursor_name());
					throw new NotImplementedException();
				} else {
					query.WhereExpression = new SqlExpressionVisitor().Visit(whereClause.conditionWrapper());
				}
			}

			return query;
		}

		#region FromClauseBuilder

		static class FromClauseBuilder {
			public static FromClause Build(PlSqlParser.FromClauseContext context) {
				if (context == null)
					return null;

				var clause = new FromClause();

				var list = context.tableRefList();
				if (list.IsEmpty)
					throw new ParseCanceledException("No source set in FROM clause");

				var tableRefs = list.tableRef().Select(FormTableRef);

				bool joinSeen = false;
				bool first = true;

				foreach (var tableRef in tableRefs) {
					if (joinSeen)
						throw new ParseCanceledException("Invalid join clause");

					var source = tableRef.Source;
					if (source.SubQuery != null) {
						clause.AddSubQuery(source.Alias, source.SubQuery);
					} else if (source.TableName != null) {
						clause.AddTable(source.Alias, source.TableName);
					}

					foreach (var joinNode in tableRef.Join) {
						var joinSource = joinNode.Source;

						if (joinSource.SubQuery != null) {
							clause.AddSubQuery(joinSource.Alias, joinSource.SubQuery);
						} else if (joinSource.TableName != null) {
							clause.AddTable(joinSource.Alias, joinSource.TableName);
						}

						clause.Join(joinNode.JoinType, joinNode.OnExpression);
						joinSeen = true;
					}

					if (!first && !joinSeen) {
						clause.Join(JoinType.Inner, null);
					}

					first = false;
				}

				return clause;
			}

			private static FromSource FormSource(PlSqlParser.Dml_table_expression_clauseContext context) {
				var tableName = Name.Object(context.objectName());
				var query = context.subquery();

				var source = new FromSource();

				if (tableName != null) {
					source.TableName = tableName.ToString();
				} else if (!query.IsEmpty) {
					source.SubQuery = Form(query);
				}

				if (context.alias != null && !context.alias.IsEmpty) {
					source.Alias = context.alias.GetText();
				}

				return source;
			}

			#region FromSource

			class FromSource {
				public SqlQueryExpression SubQuery { get; set; }

				public string TableName { get; set; }

				public string Alias { get; set; }
			}

			#endregion

			private static JoinNode FormJoinNode(PlSqlParser.JoinClauseContext context) {
				JoinType joinType;
				if (context.INNER() != null) {
					joinType = JoinType.Inner;
				} else if (!context.outerJoinType().IsEmpty) {
					if (context.outerJoinType().FULL() != null) {
						joinType = JoinType.Full;
					} else if (context.outerJoinType().LEFT() != null) {
						joinType = JoinType.Left;
					} else if (context.outerJoinType().RIGHT() != null) {
						joinType = JoinType.Right;
					} else {
						throw new ParseCanceledException("Invalid outer join type");
					}
				} else {
					throw new ParseCanceledException("Invalid join type");
				}

				var onPart = context.joinOnPart();
				if (onPart.IsEmpty)
					throw new ParseCanceledException("None ON expression found in JOIN clause");

				var onExp = new SqlExpressionVisitor().Visit(onPart.condition());
				var source = FormSource(context.dml_table_expression_clause());

				return new JoinNode {
					JoinType = joinType,
					OnExpression = onExp,
					Source = source
				};
			}

			private static TableRef FormTableRef(PlSqlParser.TableRefContext context) {
				var source = FormSource(context.dml_table_expression_clause());
				var joinNodes = context.joinClause().Select(FormJoinNode);

				return new TableRef {
					Source = source,
					Join = joinNodes.ToArray()
				};
			}

			#region TableRef

			class TableRef {
				public FromSource Source { get; set; }

				public JoinNode[] Join { get; set; }
			}

			#endregion

			#region JoinNode

			class JoinNode {
				public FromSource Source { get; set; }

				public JoinType JoinType { get; set; }

				public SqlExpression OnExpression { get; set; }
			}

			#endregion
		}

		#endregion

		#region SelectColumnBuilder

		static class SelectElement {
			public static SelectColumn BuildColumn(PlSqlParser.SelectedElementContext context) {
				string alias = null;
				if (context.column_alias() != null &&
					!context.column_alias().IsEmpty) {
					alias = Name.Simple(context.column_alias());
				}

				SelectColumn column;
				if (context.expression() != null &&
					!context.expression().IsEmpty) {
					column = new SelectColumn(Expression.Build(context.expression()), alias);
				} else if (context.selectedColumn() != null && 
					!context.selectedColumn().IsEmpty) {
					bool glob = context.selectedColumn().glob != null;
					ObjectName name = Name.Select(context.selectedColumn().objectName(), glob);

					var exp = SqlExpression.Reference(name);
					column = new SelectColumn(exp, alias);
				} else {
					throw new ParseCanceledException();
				}

				return column;
			}
		}

		#endregion
	}
}