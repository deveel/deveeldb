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

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Query {
	public sealed class QueryInfo {
		public QueryInfo(IRequest request, SqlQueryExpression expression) 
			: this(request, expression, (QueryLimit) null) {
		}

		public QueryInfo(IRequest request, SqlQueryExpression expression, IEnumerable<SortColumn> sortColumns) 
			: this(request, expression, sortColumns, null) {
		}

		public QueryInfo(IRequest request, SqlQueryExpression expression, QueryLimit limit) : this(request, expression, null, limit) {
		}

		public QueryInfo(IRequest request, SqlQueryExpression expression, IEnumerable<SortColumn> sortColumns, QueryLimit limit) {
			if (expression == null)
				throw new ArgumentNullException("expression");
			if (request == null)
				throw new ArgumentNullException("request");

			Expression = expression;
			Request = request;
			SortColumns = sortColumns;
			Limit = limit;
		}

		public SqlQueryExpression Expression { get; private set; }

		public QueryLimit Limit { get; set; }

		public IRequest Request { get; private set; }

		public IEnumerable<SortColumn> SortColumns { get; set; }
	}
}
