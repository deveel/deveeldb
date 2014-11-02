// 
//  Copyright 2010-2014 Deveel
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

using System;

namespace Deveel.Data.Sql.Expressions.Fluid {
	public static class QueryConfigurationExtensions {
		public static IQueryConfiguration All(this IQueryConfiguration configuration) {
			return configuration.All(true);
		}

		public static IQueryConfiguration Distinct(this IQueryConfiguration configuration) {
			return configuration.Distinct(true);
		}

		public static ISelectListConfiguration Column(this ISelectListConfiguration configuration, ObjectName columnName) {
			return Column(configuration, columnName, null);
		}

		public static ISelectListConfiguration Column(this ISelectListConfiguration configuration, ObjectName columnName, string alias) {
			return configuration.Expression(SqlExpression.Reference(columnName), alias);
		}

		public static ISelectListConfiguration Column(this ISelectListConfiguration configuration, string columnName) {
			return Column(configuration, columnName, null);
		}

		public static ISelectListConfiguration Column(this ISelectListConfiguration configuration, string columnName, string alias) {
			return configuration.Column(ObjectName.Parse(columnName), alias);
		}

		public static ISelectListConfiguration Expression(this ISelectListConfiguration configuration, SqlExpression expression) {
			return Expression(configuration, expression, null);
		}

		public static ISelectListConfiguration Expression(this ISelectListConfiguration configuration, SqlExpression expression, string alias) {
			return configuration.Item(x => x.Expression(expression).As(alias));
		}
	}
}