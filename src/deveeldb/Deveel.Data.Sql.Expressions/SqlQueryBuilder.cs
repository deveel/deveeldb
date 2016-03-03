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

using Deveel.Data.Sql.Fluid;

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
