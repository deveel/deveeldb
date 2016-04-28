using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Linq.Expressions;

using Deveel.Data.Util;

namespace Deveel.Data.Linq.Expressions {
	public sealed class SelectExpression : AliasedExpression {
		public SelectExpression(IEnumerable<QueryColumn> columns, Expression from, Expression where, Alias alias)
			: this(columns, @from, @where, null, null, alias) {
		}

		public SelectExpression(IEnumerable<QueryColumn> columns, Expression from, Expression where, IEnumerable<OrderExpression> orderBy, IEnumerable<Expression> groupBy, Alias alias)
			: this(false, columns, @from, @where, orderBy, groupBy, null, null, alias) {
		}

		public SelectExpression(bool distinct, IEnumerable<QueryColumn> columns, Expression from, Expression where, IEnumerable<OrderExpression> orderBy, IEnumerable<Expression> groupBy, Expression skip, Expression take, Alias alias)
			: base(QueryExpressionType.Select, typeof(void), alias) {
			Columns = columns.ToReadOnly();
			Distinct = distinct;
			From = from;
			Where = where;
			Skip = skip;
			Take = take;
			OrderBy = orderBy.ToReadOnly();
			GroupBy = groupBy.ToReadOnly();
		}

		public ReadOnlyCollection<QueryColumn> Columns { get; private set; }

		public bool Distinct { get; private set; }

		public Expression From { get; private set; }

		public Expression Where { get; private set; }

		public Expression Skip { get; private set; }

		public Expression Take { get; private set; }

		public ReadOnlyCollection<OrderExpression> OrderBy { get; private set; }

		public ReadOnlyCollection<Expression> GroupBy { get; private set; }

		public SelectExpression SetColumns(IEnumerable<QueryColumn> columns) {
			return new SelectExpression(Distinct, columns.OrderBy(c => c.Name), From, Where, OrderBy, GroupBy, Skip, Take, Alias);
		}

		public SelectExpression AddColumn(QueryColumn column) {
			var columns = new List<QueryColumn>(Columns);
			columns.Add(column);
			return SetColumns(columns);
		}

		public SelectExpression RemoveColumn(QueryColumn column) {
			var columns = new List<QueryColumn>(Columns);
			columns.Remove(column);
			return SetColumns(columns);
		}

		public string GetColumnName(string baseName) {
			string name = baseName;
			int n = 0;
			while (!IsUniqueName(Columns, name)) {
				name = baseName + (n++);
			}

			return name;
		}

		private static bool IsUniqueName(IList<QueryColumn> columns, string name) {
			foreach (var col in columns) {
				if (col.Name == name) {
					return false;
				}
			}
			return true;
		}

		public SelectExpression SetWhere(Expression where) {
			if (where != Where) {
				return new SelectExpression(Distinct, Columns, From, where, OrderBy, GroupBy, Skip, Take, Alias);
			}
			return this;
		}

		public SelectExpression SetOrderBy(IEnumerable<OrderExpression> orderBy) {
			return new SelectExpression(Distinct, Columns, From, Where, orderBy, GroupBy, Skip, Take, Alias);
		}

		public SelectExpression SetGroupBy(IEnumerable<Expression> groupBy) {
			return new SelectExpression(Distinct, Columns, From, Where, OrderBy, groupBy, Skip, Take, Alias);
		}

		public SelectExpression SetSkip(Expression skip) {
			if (skip != Skip) {
				return new SelectExpression(Distinct, Columns, From, Where, OrderBy, GroupBy, skip, Take, Alias);
			}
			return this;
		}

		public SelectExpression SetTake(Expression take) {
			if (take != Take) {
				return new SelectExpression(Distinct, Columns, From, Where, OrderBy, GroupBy, Skip, take, Alias);
			}
			return this;
		}
	}
}
