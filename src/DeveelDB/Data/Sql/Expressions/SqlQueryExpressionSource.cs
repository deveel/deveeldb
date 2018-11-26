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

namespace Deveel.Data.Sql.Expressions {
	public sealed class SqlQueryExpressionSource : ISqlExpressionPreparable<SqlQueryExpressionSource>, ISqlFormattable {
		private SqlQueryExpressionSource(ObjectName tableName, SqlQueryExpression query, string alias) {
			TableName = tableName;
			Query = query;
			Alias = alias;
		}

		public SqlQueryExpressionSource(ObjectName tableName, string alias)
			: this(tableName, null, alias) {
			if (ObjectName.IsNullOrEmpty(tableName))
				throw new ArgumentException(nameof(tableName));
		}

		public SqlQueryExpressionSource(SqlQueryExpression query, string alias)
			: this(null, query, alias) {
			if (query == null)
				throw new ArgumentNullException(nameof(query));
		}

		public string Alias { get; }

		public bool IsAliased => !String.IsNullOrWhiteSpace(Alias);

		public ObjectName TableName { get; }

		public SqlQueryExpression Query { get; }

		public bool IsQuery => Query != null;

		public bool IsTable => TableName != null;

		internal string UniqueKey { get; set; }

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			if (IsTable) {
				TableName.AppendTo(builder);
			} else {
				builder.Append("(");
				Query.AppendTo(builder);
				builder.Append(")");
			}

			if (IsAliased) {
				builder.AppendFormat(" AS {0}", Alias);
			}
		}

		public override string ToString() {
			return this.ToSqlString();
		}

		SqlQueryExpressionSource ISqlExpressionPreparable<SqlQueryExpressionSource>.Prepare(ISqlExpressionPreparer preparer) {
			var query = Query;
			if (query != null)
				query = (SqlQueryExpression) query.Prepare(preparer);

			return new SqlQueryExpressionSource(TableName, query, Alias);
		}
	}
}