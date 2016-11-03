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

using Deveel.Data.Sql.Expressions.Build;

namespace Deveel.Data.Sql.Statements.Build {
	public static class SelectOrderItemBuilderExtensions {
		public static ISelectOrderBuilder Descending(this ISelectOrderBuilder builder) {
			return builder.Direction(SortDirection.Descending);
		}

		public static ISelectOrderBuilder Ascending(this ISelectOrderBuilder builder) {
			return builder.Direction(SortDirection.Ascending);
		}

		public static ISelectOrderBuilder Expression(this ISelectOrderBuilder builder, Action<IExpressionBuilder> expression) {
			var expBuilder = new ExpressionBuilder();
			expression(expBuilder);

			return builder.Expression(expBuilder.Build());
		}

		public static ISelectOrderBuilder Column(this ISelectOrderBuilder builder, ObjectName columnName) {
			return builder.Expression(expression => expression.Reference(columnName));
		}

		public static ISelectOrderBuilder Column(this ISelectOrderBuilder builder, ObjectName parentName, string name) {
			return builder.Column(new ObjectName(parentName, name));
		}

		public static ISelectOrderBuilder Column(this ISelectOrderBuilder builder, string parentName, string name) {
			return builder.Column(ObjectName.Parse(parentName), name);
		}

		public static ISelectOrderBuilder Column(this ISelectOrderBuilder builder, string name) {
			return builder.Column(ObjectName.Parse(name));
		}
	}
}
