using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Deveel.Data.Linq.Expressions {
	class ColumnProjector : QueryExpressionVisitor {
		private readonly ITypeMapper typeMapper;
		private Dictionary<ColumnExpression, ColumnExpression> map;
		private List<QueryColumn> columns;
		private HashSet<string> columnNames;
		private HashSet<Expression> candidates;
		private HashSet<Alias> existingAliases;
		private Alias newAlias;
		private int iColumn;

		public ColumnProjector(ITypeMapper typeMapper, Expression expression, IEnumerable<QueryColumn> existingColumns, Alias newAlias, IEnumerable<Alias> existingAliases) {
			this.typeMapper = typeMapper;
			this.newAlias = newAlias;
			this.existingAliases = new HashSet<Alias>(existingAliases);
			map = new Dictionary<ColumnExpression, ColumnExpression>();

			if (existingColumns != null) {
				columns = new List<QueryColumn>(existingColumns);
				columnNames = new HashSet<string>(existingColumns.Select(c => c.Name));
			} else {
				columns = new List<QueryColumn>();
				columnNames = new HashSet<string>();
			}

			candidates = Nominator.Nominate(expression);
		}

		private bool IsColumnNameInUse(string name) {
			return columnNames.Contains(name);
		}

		private string GetUniqueColumnName(string name) {
			string baseName = name;
			int suffix = 1;
			while (IsColumnNameInUse(name)) {
				name = baseName + (suffix++);
			}
			return name;
		}

		private string GetNextColumnName() {
			return GetUniqueColumnName("c" + (iColumn++));
		}

		protected override Expression Visit(Expression expression) {
			if (candidates.Contains(expression)) {
				if (expression.NodeType == (ExpressionType)QueryExpressionType.Column) {
					ColumnExpression column = (ColumnExpression)expression;
					ColumnExpression mapped;
					if (this.map.TryGetValue(column, out mapped)) {
						return mapped;
					}

					// check for column that already refers to this column
					foreach (var existingColumn in columns) {
						ColumnExpression cex = existingColumn.Expression as ColumnExpression;
						if (cex != null && cex.Alias == column.Alias && cex.Name == column.Name) {
							// refer to the column already in the column list
							return new ColumnExpression(existingColumn.Name, column.SqlType, newAlias, column.Type);
						}
					}
					if (existingAliases.Contains(column.Alias)) {
						int ordinal = columns.Count;

						string columnName = GetUniqueColumnName(column.Name);
						columns.Add(new QueryColumn(columnName, column, column.SqlType));
						mapped = new ColumnExpression(columnName, column.SqlType, newAlias, column.Type);

						map.Add(column, mapped);
						columnNames.Add(columnName);
						return mapped;
					}

					// must be referring to outer scope
					return column;
				} else {
					string columnName = GetNextColumnName();
					var colType = typeMapper.MapToSqlType(expression.Type);
					columns.Add(new QueryColumn(columnName, expression, colType));
					return new ColumnExpression(columnName, colType, newAlias, expression.Type);
				}
			}

			return base.Visit(expression);
		}

		public static ProjectedColumns ProjectColumns(ITypeMapper typeMapper, Expression expression, IEnumerable<QueryColumn> existingColumns, Alias newAlias, IEnumerable<Alias> existingAliases) {
			ColumnProjector projector = new ColumnProjector(typeMapper, expression, existingColumns, newAlias, existingAliases);
			Expression expr = projector.Visit(expression);
			return new ProjectedColumns(expr, projector.columns.AsReadOnly());
		}

		public static ProjectedColumns ProjectColumns(ITypeMapper typeMapper, Expression expression, IEnumerable<QueryColumn> existingColumns, Alias newAlias, params Alias[] existingAliases) {
			return ProjectColumns(typeMapper, expression, existingColumns, newAlias, (IEnumerable<Alias>)existingAliases);
		}


		#region Nominator

		class Nominator : QueryExpressionVisitor {
			private bool blocked;
			private readonly HashSet<Expression> candidates;

			public Nominator() {
				candidates = new HashSet<Expression>();
			}

			public static HashSet<Expression> Nominate(Expression expression) {
				var nominator = new Nominator();
				nominator.Visit(expression);
				return nominator.candidates;
			}

			private bool IsColumn(Expression expression) {
				switch (expression.NodeType) {
					case (ExpressionType)QueryExpressionType.Column:
					case (ExpressionType)QueryExpressionType.Scalar:
					case (ExpressionType)QueryExpressionType.Exists:
					case (ExpressionType)QueryExpressionType.AggregateSubquery:
					case (ExpressionType)QueryExpressionType.Aggregate:
						return true;
					default:
						return false;
				}
			}

			protected override Expression Visit(Expression expression) {
				bool saveIsBlocked = blocked;
				blocked = false;
				if (IsColumn(expression)) {
					candidates.Add(expression);
					// don't merge saveIsBlocked
				} else {
					base.Visit(expression);

					if (!blocked) {
						if (IsColumn(expression)) {
							candidates.Add(expression);
						} else {
							blocked = true;
						}
					}

					blocked |= saveIsBlocked;
				}

				return expression;
			}
		}

		#endregion
	}
}
