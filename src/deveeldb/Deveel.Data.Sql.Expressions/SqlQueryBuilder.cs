using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Expressions.Fluid;

namespace Deveel.Data.Sql.Expressions {
	public static class SqlQueryBuilder {
		public static IQueryConfiguration Configure() {
			return new QueryConfiguration();
		}

		#region QueryConfiguration

		class QueryConfiguration : IQueryConfiguration {
			public QueryConfiguration() {
				ItemList = new SelectListConfiguration();
			}

			public bool IsAll { get; private set; }

			public bool IsDistinct { get; private set; }

			public ISelectListConfiguration ItemList { get; private set; }

			public SqlExpression HavingExpression { get; private set; }

			public SqlExpression WhereExpression { get; private set; }

			public IQueryConfiguration All(bool flag) {
				IsAll = flag;
				return this;
			}

			public IQueryConfiguration Distinct(bool flag) {
				IsDistinct = flag;
				return this;
			}

			public IQueryConfiguration Items(Action<ISelectListConfiguration> config) {
				if (config != null)
					config(ItemList);

				return this;
			}

			public IQueryConfiguration From(Action<IFromSourceConfiguration> config) {
				throw new NotImplementedException();
			}

			public IQueryConfiguration Where(SqlExpression whereExpression) {
				WhereExpression = whereExpression;
				return this;
			}

			public IQueryConfiguration Having(SqlExpression havingExpression) {
				HavingExpression = havingExpression;
				return this;
			}

			public IQueryConfiguration GroupBy(Action<IGroupByConfiguration> config) {
				throw new NotImplementedException();
			}

			public IQueryConfiguration OrderBy(Action<IOrderByConfiguration> config) {
				throw new NotImplementedException();
			}

			public SqlQueryExpression AsExpression() {
				throw new NotImplementedException();
			}
		}

		#endregion

		#region ISelectListConfiguration

		class SelectListConfiguration : ISelectListConfiguration {
			private readonly List<SelectItemConfiguration> items;

			public SelectListConfiguration() {
				items = new List<SelectItemConfiguration>();
			}

			public ISelectListConfiguration Item(Action<ISelectItemConfiguration> config) {
				if (config != null) {
					var item = new SelectItemConfiguration();
					config(item);
					items.Add(item);
				}

				return this;
			}

			public IEnumerable<ISelectItemConfiguration> Items {
				get { return items.Cast<ISelectItemConfiguration>().AsEnumerable(); }
			}
		}

		#endregion

		#region SelectItemConfiguration

		class SelectItemConfiguration : ISelectItemConfiguration, ISelectItemWithExpressionConfiguration {
			public ISelectItemWithExpressionConfiguration Expression(SqlExpression itemExpression) {
				ItemExpression = itemExpression;
				return this;
			}

			public SqlExpression ItemExpression { get; private set; }

			public string ItemAlias { get; private set; }

			public void As(string alias) {
				ItemAlias = alias;
			}
		}

		#endregion
	}
}
