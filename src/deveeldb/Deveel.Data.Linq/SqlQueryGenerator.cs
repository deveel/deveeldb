using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

using Antlr4.Runtime;

using Deveel.Data.Mapping;
using Deveel.Data.Sql;

using Remotion.Linq;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.Clauses.ResultOperators;
using Remotion.Linq.Parsing;

namespace Deveel.Data.Linq {
	class SqlQueryGenerator : QueryModelVisitorBase {
		private SqlQueryBuilder sql;
		private List<QueryParameter> parameters = new List<QueryParameter>();
		private ExpressionCompileContext buildContext;
		private IDictionary<string, string> uniqueSourceNames = new Dictionary<string, string>();

		private SqlQueryGenerator(IQuery query) {
			var model = query.CompileModel();
			buildContext = new ExpressionCompileContext(model);
			sql = new SqlQueryBuilder(buildContext);
		}

		public static SqlQuery GenerateSqlQuery(IQuery query, QueryModel queryModel) {
			var visitor = new SqlQueryGenerator(query);
			visitor.VisitQueryModel(queryModel);
			return visitor.GetQuery();
		}

		private string GetSqlExpression(Expression expression) {
			return SqlGeneratorExpressionVisitor.GetSqlExpression(expression, buildContext, parameters);
		}

		private string GetSqlSelectExpression(Expression selectExpression) {
			return SqlSelectGeneratorExpressionVisitor.GetSqlExpression(selectExpression, buildContext);
		}

		private static IDictionary<string, string> DiscoverSources(MainFromClause fromClause) {
			var visitor = new SourceDiscoveryVisitor();
			visitor.Visit(fromClause.FromExpression);

			var sources = visitor.GetSourceNames();
			if (!sources.ContainsKey(fromClause.ItemName)) {
				sources.Add(fromClause.ItemName, "t0");
			}

			return sources;
		}

		public override void VisitQueryModel(QueryModel queryModel) {
			uniqueSourceNames = DiscoverSources(queryModel.MainFromClause);

			queryModel.SelectClause.Accept(this, queryModel);
			queryModel.MainFromClause.Accept(this, queryModel);
			VisitBodyClauses(queryModel.BodyClauses, queryModel);
			VisitResultOperators(queryModel.ResultOperators, queryModel);
		}

		public SqlQuery GetQuery() {
			var query = new SqlQuery(sql.ToString());
			foreach (var parameter in parameters) {
				query.Parameters.Add(parameter);
			}

			return query;
		}

		public override void VisitResultOperator(ResultOperatorBase resultOperator, QueryModel queryModel, int index) {
			// TODO: continue all
			if (resultOperator is CountResultOperator) {
				sql.AddSelect(String.Format("CAST(COUNT({0}) AS INTEGER)", sql.GetSelectAt(index)));
			} else if (resultOperator is MinResultOperator) {
				sql.AddSelect(String.Format("CAST(MIN({0}) AS INTEGER)", sql.GetSelectAt(index)));
			} else if (resultOperator is MaxResultOperator) {
				// TODO:
			} else if (resultOperator is AverageResultOperator) { 
				// TODO:
			} else if (resultOperator is AllResultOperator) {
				var all = (AllResultOperator) resultOperator;
				sql.AddSelect(GetSqlExpression(all.Predicate));
			}

			base.VisitResultOperator(resultOperator, queryModel, index);
		}

		public override void VisitMainFromClause(MainFromClause fromClause, QueryModel queryModel) {
			string name;
			if (!uniqueSourceNames.TryGetValue(fromClause.ItemName, out name))
				throw new NotSupportedException();

			sql.AddFrom(fromClause.ItemType, name);
			base.VisitMainFromClause(fromClause, queryModel);
		}

		public override void VisitSelectClause(SelectClause selectClause, QueryModel queryModel) {
			sql.AddSelect(GetSqlSelectExpression(selectClause.Selector));

			base.VisitSelectClause(selectClause, queryModel);
		}

		public override void VisitWhereClause(WhereClause whereClause, QueryModel queryModel, int index) {
			sql.AddWhere(GetSqlExpression(whereClause.Predicate));

			base.VisitWhereClause(whereClause, queryModel, index);
		}

		public override void VisitOrderByClause(OrderByClause orderByClause, QueryModel queryModel, int index) {
			sql.AddOrderBy(orderByClause.Orderings.Select(o => GetSqlExpression(o.Expression)));

			base.VisitOrderByClause(orderByClause, queryModel, index);
		}

		public override void VisitJoinClause(JoinClause joinClause, QueryModel queryModel, int index) {
			string name;
			if (!uniqueSourceNames.TryGetValue(joinClause.ItemName, out name))
				throw new NotSupportedException();

			sql.AddFrom(joinClause.ItemType, name);
			sql.AddWhere(
				"({0} = {1})",
				GetSqlExpression(joinClause.OuterKeySelector),
				GetSqlExpression(joinClause.InnerKeySelector));

			base.VisitJoinClause(joinClause, queryModel, index);
		}

		public override void VisitAdditionalFromClause(AdditionalFromClause fromClause, QueryModel queryModel, int index) {
			string name;
			if (!uniqueSourceNames.TryGetValue(fromClause.ItemName, out name))
				throw new NotSupportedException();

			sql.AddFrom(fromClause.ItemType, name);

			base.VisitAdditionalFromClause(fromClause, queryModel, index);
		}

		#region SourceDiscoveryVisitor

		class SourceDiscoveryVisitor : RelinqExpressionVisitor {
			private Dictionary<string, string> sourceNames;
			private int sourceCount = 0;

			public SourceDiscoveryVisitor() {
				sourceNames = new Dictionary<string, string>();
			}

			public IDictionary<string, string> GetSourceNames() {
				return sourceNames;
			}

			protected override Expression VisitQuerySourceReference(QuerySourceReferenceExpression expression) {
				var source = expression.ReferencedQuerySource;
				if (!sourceNames.ContainsKey(source.ItemName)) {
					var name = String.Format("t{0}", ++sourceCount);
					sourceNames.Add(source.ItemName, name);
				}

				return expression;
			}
		}

		#endregion

		#region SqlQueryBuilder

		class SqlQueryBuilder {
			public SqlQueryBuilder(ExpressionCompileContext context) {
				Context = context;
				Select = new List<string>();
				From = new List<string>();
				Where = new List<string>();
				OrderBy = new List<string>();
			}

			public ExpressionCompileContext Context { get; private set; }


			private IList<string> Select { get; set; }

			private ICollection<string> From { get; set; }

			private ICollection<string> Where { get; set; }

			private ICollection<string> OrderBy { get; set; }

			private string GetObjectName(Type type) {
				return Context.FindTableName(type);
			}

			public void AddSelect(string value) {
				Select.Add(value);
			}

			public string GetSelectAt(int index) {
				return Select[index];
			}

			public void AddFrom(Type itemType, string alias) {
				var sb = new StringBuilder();
				var objName = GetObjectName(itemType);
				sb.Append(objName);

				if (!String.IsNullOrEmpty(alias))
					sb.AppendFormat(" AS {0}", alias);

				From.Add(sb.ToString());
			}

			public void AddWhere(string format, params string[] args) {
				Where.Add(String.Format(format, args));
			}

			public void AddOrderBy(IEnumerable<string> orderBy) {
				foreach (var o in orderBy) {
					OrderBy.Add(o);
				}
			}

			public override string ToString() {
				var stringBuilder = new StringBuilder();

				if (Select.Count == 0 || 
					From.Count == 0)
					throw new InvalidOperationException("A query must have a select part and at least one from part.");

				stringBuilder.AppendFormat("SELECT {0}", String.Join(", ", Select.ToArray()));
				stringBuilder.AppendFormat(" FROM {0}", String.Join(", ", From.ToArray()));

				if (Where.Count > 0)
					stringBuilder.AppendFormat(" WHERE {0}", string.Join(" AND ", Where.ToArray()));

				if (OrderBy.Count > 0)
					stringBuilder.AppendFormat(" ORDER BY {0}", String.Join(", ", OrderBy.ToArray()));

				return stringBuilder.ToString();
			}
		}

		#endregion
	}
}
