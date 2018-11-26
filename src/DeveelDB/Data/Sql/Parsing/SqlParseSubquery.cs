// 
//  Copyright 2010-2018 Deveel
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

using Deveel.Data.Query;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Parsing {
	static class SqlParseSubquery {
		public static SqlQueryExpression Form(IContext context, PlSqlParser.SubqueryContext subquery) {
			SqlParseIntoClause into;
			return Form(context, subquery, out into);
		}

		public static SqlQueryExpression Form(IContext context, PlSqlParser.SubqueryContext subquery, out SqlParseIntoClause into) {
			var query = Form(context, subquery.subqueryBasicElements(), out into);

			var opPart = subquery.subquery_operation_part();

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

					var next = Form(context, part.subqueryBasicElements());
					var prev = query.NextComposite;

					if (prev == null) {
						prev = new SqlQueryExpressionComposite(function, isAll, next);
					} else {
                        prev.Expression.NextComposite = new SqlQueryExpressionComposite(function, isAll, next);
					}

				    query.NextComposite = prev;
				}
			}

			return query;
		}

		private static SqlQueryExpression Form(IContext context, PlSqlParser.SubqueryBasicElementsContext basicElements) {
			SqlParseIntoClause into;
			return Form(context, basicElements, out into);
		}

		private static SqlQueryExpression Form(IContext context, PlSqlParser.SubqueryBasicElementsContext basicElements, out SqlParseIntoClause into) {
			var sub = basicElements.subquery();
			if (sub != null && !sub.IsEmpty)
				return Form(context, sub, out into);

			return Form(context, basicElements.queryBlock(), out into);
		}

		private static SqlQueryExpression Form(IContext context, PlSqlParser.QueryBlockContext queryBlock, out SqlParseIntoClause into) {
			var fromClause = FromClauseBuilder.Build(context, queryBlock.fromClause());

			SqlQueryExpressionItem[] columns;

			if (queryBlock.all != null) {
				columns = new[] {new SqlQueryExpressionItem(SqlExpression.Reference(new ObjectName("*")))};
			} else {
				columns = queryBlock.selectedElement().Select(x => SelectElement.BuildColumn(context, x)).ToArray();
			}

		    var query = new SqlQueryExpression();

		    foreach (var column in columns) {
		        query.Items.Add(column);
		    }

			into = null;

			if (queryBlock.DISTINCT() != null ||
				queryBlock.UNIQUE() != null)
				query.Distinct = true;

			var intoClause = queryBlock.into_clause();
			if (intoClause != null) {
				into = new SqlParseIntoClause();

				if (intoClause.objectName() != null) {
					into.TableName = SqlParseName.Object(intoClause.objectName());
				} else if (intoClause.variable_name() != null) {
					into.Variables = intoClause.variable_name().Select(SqlParseName.Variable).ToArray();
				}
			}

			if (fromClause != null)
				query.From = fromClause;

			var groupBy = queryBlock.groupByClause();
			if (groupBy != null && !groupBy.IsEmpty) {
				query.GroupBy = groupBy.groupByElements().expression().Select(x => new SqlExpressionVisitor(context).Visit(x)).ToList();

				var having = groupBy.havingClause();
				if (having != null)
					query.Having = new SqlExpressionVisitor(context).Visit(having.condition());
			}

			var groupMax = queryBlock.groupMaxClause();
			if (groupMax != null && !groupMax.IsEmpty) {
				var maxColumn = SqlParseName.Object(groupMax.objectName());
				query.GroupMax = maxColumn;
			}

			var whereClause = queryBlock.whereClause();
			if (whereClause != null && !whereClause.IsEmpty) {
				var currentOf = whereClause.current_of_clause();
				if (currentOf != null && !currentOf.IsEmpty) {
					var cursorName = SqlParseName.Simple(currentOf.cursor_name());
					throw new NotImplementedException();
				} else {
					query.Where = new SqlExpressionVisitor(context).Visit(whereClause.conditionWrapper());
				}
			}

		    //TODO: in case of a SELECT INTO cause create a statement

            return query;
		}

		#region FromClauseBuilder

		static class FromClauseBuilder {
			public static SqlQueryExpressionFrom Build(IContext context, PlSqlParser.FromClauseContext fromClause) {
				if (fromClause == null)
					return null;

				var clause = new SqlQueryExpressionFrom();

				var list = fromClause.tableRefList();
				if (list.IsEmpty)
					throw new ParseCanceledException("No source set in FROM clause");

				var tableRefs = list.tableRef().Select(x => FormTableRef(context, x));

				bool joinSeen = false;
				bool first = true;

				foreach (var tableRef in tableRefs) {
					if (joinSeen)
						throw new ParseCanceledException("Invalid join clause");

					var source = tableRef.Source;
					if (source.SubQuery != null) {
						clause.Query(source.SubQuery, source.Alias);
					} else if (source.TableName != null) {
						clause.Table(source.TableName, source.Alias);
					}

					foreach (var joinNode in tableRef.Join) {
						var joinSource = joinNode.Source;

						if (joinSource.SubQuery != null) {
							clause.Query(joinSource.SubQuery, joinSource.Alias);
						} else if (joinSource.TableName != null) {
							clause.Table(joinSource.TableName, joinSource.Alias);
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

			private static FromSource FormSource(IContext context, PlSqlParser.QueryExpressionClauseContext clause) {
				var tableName = SqlParseName.Object(clause.objectName());
				var query = clause.subquery();

				var source = new FromSource();

				if (tableName != null) {
					source.TableName = ObjectName.Parse(tableName.ToString());
				} else if (!query.IsEmpty) {
					source.SubQuery = Form(context, query);
				}

				if (clause.alias != null && !clause.alias.IsEmpty) {
					source.Alias = clause.alias.GetText();
				}

				return source;
			}

			#region FromSource

			class FromSource {
				public SqlQueryExpression SubQuery { get; set; }

				public ObjectName TableName { get; set; }

				public string Alias { get; set; }
			}

			#endregion

			private static JoinNode FormJoinNode(IContext context, PlSqlParser.JoinClauseContext joinClause) {
				JoinType joinType;
				if (joinClause.INNER() != null) {
					joinType = JoinType.Inner;
				} else if (joinClause.outerJoinType() != null) {
					if (joinClause.outerJoinType().FULL() != null) {
						joinType = JoinType.Full;
					} else if (joinClause.outerJoinType().LEFT() != null) {
						joinType = JoinType.Left;
					} else if (joinClause.outerJoinType().RIGHT() != null) {
						joinType = JoinType.Right;
					} else {
						throw new ParseCanceledException("Invalid outer join type");
					}
				} else {
					throw new ParseCanceledException("Invalid join type");
				}

				var onPart = joinClause.joinOnPart();
				if (onPart.IsEmpty)
					throw new ParseCanceledException("None ON expression found in JOIN clause");

				var onExp = new SqlExpressionVisitor(context).Visit(onPart.condition());
				var source = FormSource(context, joinClause.queryExpressionClause());

				return new JoinNode {
					JoinType = joinType,
					OnExpression = onExp,
					Source = source
				};
			}

			private static TableRef FormTableRef(IContext context, PlSqlParser.TableRefContext tableRef) {
				var source = FormSource(context, tableRef.queryExpressionClause());
				var joinNodes = tableRef.joinClause().Select(x => FormJoinNode(context, x));

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
			public static SqlQueryExpressionItem BuildColumn(IContext context, PlSqlParser.SelectedElementContext selectedElement) {
				string alias = null;
				if (selectedElement.column_alias() != null &&
					!selectedElement.column_alias().IsEmpty) {
					alias = SqlParseName.Simple(selectedElement.column_alias());
				}

			    SqlQueryExpressionItem column;
				if (selectedElement.expression() != null &&
					!selectedElement.expression().IsEmpty) {
					column = new SqlQueryExpressionItem(SqlParseExpression.Build(context, selectedElement.expression()), alias);
				} else if (selectedElement.selectedColumn() != null && 
					!selectedElement.selectedColumn().IsEmpty) {
					bool glob = selectedElement.selectedColumn().glob != null;
					ObjectName name = SqlParseName.Select(selectedElement.selectedColumn().objectName(), glob);

					var exp = SqlExpression.Reference(name);
					column = new SqlQueryExpressionItem(exp, alias);
				} else {
					throw new ParseCanceledException();
				}

				return column;
			}
		}

		#endregion
	}
}