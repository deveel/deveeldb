using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using Deveel.Data.Linq.Expressions;

namespace Deveel.Data.Linq {
	class QueryLanguage {
		public QueryLanguage(ITypeMapper typeSystem) {
			TypeSystem = typeSystem;
		}

		public ITypeMapper TypeSystem { get; private set; }

		public Expression GetGeneratedIdExpression(MemberInfo member) {
			throw new NotImplementedException();
		}

		public bool AllowsMultipleCommands {
			get { return true; }
		}

		public bool AllowSubqueryInSelectWithoutFrom {
			get { return false; }
		}

		public bool AllowDistinctInAggregates {
			get { return true; }
		}

		public Expression GetRowsAffectedExpression(Expression command) {
			throw new NotImplementedException();
		}

		public bool IsRowsAffectedExpressions(Expression expression) {
			throw new NotImplementedException();
		}

		public Expression GetOuterJoinTest(SelectExpression select) {
			// if the column is used in the join condition (equality test)
			// if it is null in the database then the join test won't match (null != null) so the row won't appear
			// we can safely use this existing column as our test to determine if the outer join produced a row

			// find a column that is used in equality test
			var aliases = DeclaredAliasGatherer.Gather(select.From);
			var joinColumns = JoinColumnGatherer.Gather(aliases, select).ToList();
			if (joinColumns.Count > 0) {
				// prefer one that is already in the projection list.
				foreach (var jc in joinColumns) {
					foreach (var col in select.Columns) {
						if (jc.Equals(col.Expression)) {
							return jc;
						}
					}
				}
				return joinColumns[0];
			}

			// fall back to introducing a constant
			return Expression.Constant(1, typeof(int?));
		}

		public ProjectionExpression AddOuterJoinTest(ProjectionExpression proj) {
			var test = this.GetOuterJoinTest(proj.Source);
			var select = proj.Source;
			ColumnExpression testCol = null;

			// look to see if test expression exists in columns already
			foreach (var col in select.Columns) {
				if (test.Equals(col.Expression)) {
					var colType = this.TypeSystem.MapToSqlType(test.Type);
					testCol = new ColumnExpression(col.Name, colType, select.Alias, test.Type);
					break;
				}
			}
			if (testCol == null) {
				// add expression to projection
				testCol = test as ColumnExpression;
				string colName = (testCol != null) ? testCol.Name : "Test";
				colName = proj.Source.GetColumnName(colName);
				var colType = TypeSystem.MapToSqlType(test.Type);
				select = select.AddColumn(new QueryColumn(colName, test, colType));
				testCol = new ColumnExpression(colName, colType, select.Alias, test.Type);
			}

			var newProjector = new OuterJoinedExpression(testCol, proj.Projector);
			return new ProjectionExpression(select, newProjector, proj.Aggregate);
		}

		public bool IsAggregate(MemberInfo member) {
			var method = member as MethodInfo;
			if (method != null) {
				if (method.DeclaringType == typeof(Queryable)
				    || method.DeclaringType == typeof(Enumerable)) {
					switch (method.Name) {
						case "Count":
						case "LongCount":
						case "Sum":
						case "Min":
						case "Max":
						case "Average":
							return true;
					}
				}
			}
			var property = member as PropertyInfo;
			if (property != null
			    && property.Name == "Count"
			    && typeof(IEnumerable).IsAssignableFrom(property.DeclaringType)) {
				return true;
			}
			return false;
		}

		public bool AggregateArgumentIsPredicate(string aggregateName) {
			return aggregateName == "Count" || aggregateName == "LongCount";
		}

		public virtual bool CanBeColumn(Expression expression) {
			// by default, push all work in projection to client
			return this.MustBeColumn(expression);
		}

		public virtual bool MustBeColumn(Expression expression) {
			switch (expression.NodeType) {
				case (ExpressionType) QueryExpressionType.Column:
				case (ExpressionType) QueryExpressionType.Scalar:
				case (ExpressionType) QueryExpressionType.Exists:
				case (ExpressionType) QueryExpressionType.AggregateSubquery:
				case (ExpressionType) QueryExpressionType.Aggregate:
					return true;
				default:
					return false;
			}
		}

		#region JoinColumnGatherer

		class JoinColumnGatherer {
			private readonly HashSet<Alias> aliases;
			private readonly HashSet<ColumnExpression> columns = new HashSet<ColumnExpression>();

			private JoinColumnGatherer(HashSet<Alias> aliases) {
				this.aliases = aliases;
			}

			public static HashSet<ColumnExpression> Gather(HashSet<Alias> aliases, SelectExpression select) {
				var gatherer = new JoinColumnGatherer(aliases);
				gatherer.Gather(select.Where);
				return gatherer.columns;
			}

			private void Gather(Expression expression) {
				BinaryExpression b = expression as BinaryExpression;
				if (b != null) {
					switch (b.NodeType) {
						case ExpressionType.Equal:
						case ExpressionType.NotEqual:
							if (IsExternalColumn(b.Left) && GetColumn(b.Right) != null) {
								columns.Add(GetColumn(b.Right));
							} else if (IsExternalColumn(b.Right) && GetColumn(b.Left) != null) {
								columns.Add(GetColumn(b.Left));
							}
							break;
						case ExpressionType.And:
						case ExpressionType.AndAlso:
							if (b.Type == typeof(bool) || b.Type == typeof(bool?)) {
								Gather(b.Left);
								Gather(b.Right);
							}
							break;
					}
				}
			}


			private ColumnExpression GetColumn(Expression exp) {
				while (exp.NodeType == ExpressionType.Convert)
					exp = ((UnaryExpression)exp).Operand;
				return exp as ColumnExpression;
			}

			private bool IsExternalColumn(Expression exp) {
				var col = GetColumn(exp);
				if (col != null && !aliases.Contains((Alias)col.Alias))
					return true;
				return false;
			}
		}

		#endregion
	}
}
