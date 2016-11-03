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
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Sql.Expressions.Build {
	class QueryExpressionBuilder : IQueryExpressionBuilder {
		private List<QueryExpressionItemBuilder> items;
		private List<QueryExpressionSourceBuilder> querySources;
		private SqlExpression filterExpression;
		private List<SqlExpression> groupByExpressions;
		private ObjectName groupMax;
		private bool distinct;
		private byte filterType;

		private const byte WhereFilter = 1;
		private const byte HavingFilter = 2;

		public QueryExpressionBuilder() {
			items = new List<QueryExpressionItemBuilder>();
			groupByExpressions = new List<SqlExpression>();
			querySources = new List<QueryExpressionSourceBuilder>();
		}

		public IQueryExpressionBuilder Distinct(bool value = true) {
			distinct = value;
			return this;
		}

		public IQueryExpressionBuilder Item(Action<IQueryExpressionItemBuilder> item) {
			var builder = new QueryExpressionItemBuilder();
			item(builder);

			items.Add(builder);

			return this;
		}

		public IQueryExpressionBuilder From(params Action<IQueryExpressionSourceBuilder>[] sources) {
			foreach (var source in sources) {
				var querySource = new QueryExpressionSourceBuilder();
				source(querySource);

				querySources.Add(querySource);
			}

			return this;
		}

		public IQueryExpressionBuilder GroupBy(params SqlExpression[] groupBy) {
			if (groupBy != null) {
				foreach (var expression in groupBy) {
					if (expression == null)
						throw new ArgumentNullException();

					groupByExpressions.Add(expression);
				}
			}

			return this;
		}

		public IQueryExpressionBuilder GroupMax(ObjectName columnName) {
			groupMax = columnName;
			return this;
		}

		public IQueryExpressionBuilder Where(SqlExpression where) {
			filterExpression = where;
			filterType = WhereFilter;

			return this;
		}

		public IQueryExpressionBuilder Having(SqlExpression having) {
			filterExpression = having;
			filterType = HavingFilter;

			return this;
		}

		public SqlQueryExpression Build() {
			var query = new SqlQueryExpression(items.Select(x => x.Build()));

			if (querySources.Count > 0) {
				foreach (var source in querySources) {
					source.BuildIn(query);
				}
			}

			query.Distinct = distinct;

			if (groupByExpressions.Count > 0) {
				query.GroupBy = groupByExpressions.AsEnumerable();
			}

			if (groupMax != null)
				query.GroupMax = groupMax;

			if (filterType == WhereFilter) {
				query.WhereExpression = filterExpression;
			} else if (filterType == HavingFilter) {
				query.HavingExpression = filterExpression;
			}

			return query;
		}

		#region QueryExpressionItemBuilder

		class QueryExpressionItemBuilder : IQueryExpressionItemBuilder {
			private SqlExpression itemExpression;
			private string itemAlias;

			public IQueryExpressionItemBuilder Expression(SqlExpression expression) {
				if (expression == null)
					throw new ArgumentNullException("expression");

				itemExpression = expression;
				return this;
			}

			public IQueryExpressionItemBuilder As(string alias) {
				itemAlias = alias;
				return this;
			}

			public SelectColumn Build() {
				if (itemExpression == null)
					throw new InvalidOperationException();

				return new SelectColumn(itemExpression, itemAlias);
			}
		}

		#endregion

		#region QueryExpressionSourceBuilder

		class QueryExpressionSourceBuilder : IQueryExpressionSourceBuilder {
			private byte sourceType;
			private ObjectName sourceTableName;
			private QueryExpressionBuilder sourceQuery;
			private string aliasName;
			private QueryExpressionSourceJoinBuilder joinBuilder;

			private const byte UnknownSource = 0;
			private const byte TableSource = 1;
			private const byte QuerySource = 2;

			public IQueryExpressionSourceBuilder Table(ObjectName tableName) {
				if (sourceType != UnknownSource)
					throw new ArgumentException("A source was already set.");
				if (tableName == null)
					throw new ArgumentNullException("tableName");

				sourceType = TableSource;
				sourceTableName = tableName;
				return this;
			}

			public IQueryExpressionSourceBuilder Query(Action<IQueryExpressionBuilder> query) {
				if (sourceType != UnknownSource)
					throw new ArgumentException();

				sourceQuery = new QueryExpressionBuilder();
				query(sourceQuery);
				sourceType = QuerySource;

				return this;
			}

			public IQueryExpressionSourceBuilder As(string alias) {
				aliasName = alias;
				return this;
			}

			public IQueryExpressionSourceBuilder Join(Action<IQueryExpressionSourceJoinBuilder> join) {
				joinBuilder = new QueryExpressionSourceJoinBuilder();
				join(joinBuilder);

				return this;
			}

			public void BuildIn(SqlQueryExpression query) {
				if (sourceType == TableSource) {
					query.FromClause.AddTable(aliasName, sourceTableName.ToString());
				} else if (sourceType == QuerySource) {
					var subQuery = sourceQuery.Build();
					query.FromClause.AddSubQuery(aliasName, subQuery);
				} else if (sourceType == UnknownSource) {
					throw new InvalidOperationException("Unknown source type");
				}

				if (joinBuilder != null)
					joinBuilder.BuildIn(query);
			}

			#region QueryExpressionSourceJoinBuilder

			class QueryExpressionSourceJoinBuilder : IQueryExpressionSourceJoinBuilder {
				private QueryExpressionSourceBuilder sourceBuilder;
				private JoinType sourceJoinType;
				private SqlExpression joinExpression;

				public IQueryExpressionSourceJoinBuilder Source(Action<IQueryExpressionSourceBuilder> source) {
					sourceBuilder = new QueryExpressionSourceBuilder();
					source(sourceBuilder);

					return this;
				}

				public IQueryExpressionSourceJoinBuilder JoinType(JoinType joinType) {
					sourceJoinType = joinType;
					return this;
				}

				public IQueryExpressionSourceJoinBuilder On(SqlExpression expression) {
					joinExpression = expression;
					return this;
				}

				public void BuildIn(SqlQueryExpression query) {
					sourceBuilder.BuildIn(query);
					query.FromClause.Join(sourceJoinType, joinExpression);
				}
			}

			#endregion
		}

		#endregion
	}
}
