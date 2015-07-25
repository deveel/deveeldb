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
using System.Collections.ObjectModel;
using System.Linq;

namespace Deveel.Data.Sql {
	public sealed class SqlQuery {
		public SqlQuery(string text) 
			: this(text, QueryParameterStyle.Default) {
		}

		public SqlQuery(string text, QueryParameterStyle parameterStyle) {
			Text = text;
			ParameterStyle = parameterStyle;
			Parameters = new QueryParameterCollection(this);
		}

		public string Text { get; private set; }

		public ICollection<QueryParameter> Parameters { get; private set; }

		public QueryParameterStyle ParameterStyle { get; private set; }

		internal void ChangeStyle(QueryParameterStyle style) {
			if (ParameterStyle != QueryParameterStyle.Default)
				throw new InvalidOperationException();
			if (style == QueryParameterStyle.Default)
				throw new ArgumentException();

			ParameterStyle = style;
		}

		#region QueryParameterCollection

		class QueryParameterCollection : Collection<QueryParameter> {
			private SqlQuery SqlQuery { get; set; }

			public QueryParameterCollection(SqlQuery sqlQuery) {
				SqlQuery = sqlQuery;
			}

			private void ValidateParameter(QueryParameter item) {
				if (item == null)
					throw new ArgumentNullException("item");

				if (SqlQuery.ParameterStyle == QueryParameterStyle.Marker &&
					!String.Equals(item.Name, QueryParameter.Marker, StringComparison.Ordinal))
					throw new ArgumentException(String.Format("The query accepts markers, but the parameter '{0}' is named.", item.Name));
				if (SqlQuery.ParameterStyle == QueryParameterStyle.Named) {
					if (item.Name.Equals(QueryParameter.Marker, StringComparison.Ordinal))
						throw new ArgumentException("The query accepts named parameters, but a marker was set.");

					if (Items.Any(x => String.Equals(x.Name, item.Name)))
						throw new ArgumentException(String.Format("A parameter named {0} was already inserted in the query.", item.Name));
				}
			}

			protected override void InsertItem(int index, QueryParameter item) {
				ValidateParameter(item);
				base.InsertItem(index, item);
			}

			protected override void SetItem(int index, QueryParameter item) {
				ValidateParameter(item);
				base.SetItem(index, item);
			}
		}

		#endregion
	}
}