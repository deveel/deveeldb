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
using System.Diagnostics.Eventing.Reader;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Query {
	static class QueryExpressionExtensions {
		public static void DiscoverTableNames(this SqlExpression expression, IList<ObjectName> tableNames) {
			var visitor = new TableNamesVisitor(tableNames);
			visitor.Visit(expression);
		}

		public static IList<QueryReference> DiscoverQueryReferences(this SqlExpression expression, ref int level, IList<QueryReference> list) {
			var visitor = new QueryReferencesVisitor(list, level);
			visitor.Visit(expression);
			level = visitor.Level;
			return visitor.References;
		}

		public static IEnumerable<ObjectName> DiscoverColumnNames(this SqlExpression expression) {
			var discovery = new ColumnNameDiscovery();
			return discovery.Discover(expression);
		}

		#region ColumnNameDiscovery

		class ColumnNameDiscovery : SqlExpressionVisitor {
			private readonly List<ObjectName> columnNames;

			public ColumnNameDiscovery() {
				columnNames = new List<ObjectName>();
			}

			public IEnumerable<ObjectName> Discover(SqlExpression expression) {
				Visit(expression);
				return columnNames.AsReadOnly();
			}

			public override SqlExpression VisitConstant(SqlConstantExpression constant) {
				var value = constant.Value;
				if (value.Type is ArrayType) {
					var array = (SqlArray) value.Value;
					foreach (var element in array) {
						columnNames.AddRange(element.DiscoverColumnNames());
					}
				}

				return base.VisitConstant(constant);
			}

			public override SqlExpression VisitFunctionCall(SqlFunctionCallExpression expression) {
				var args = expression.Arguments;
				foreach (var arg in args) {
					columnNames.AddRange(arg.DiscoverColumnNames());
				}

				return base.VisitFunctionCall(expression);
			}

			public override SqlExpression VisitReference(SqlReferenceExpression reference) {
				columnNames.Add(reference.ReferenceName);
				return base.VisitReference(reference);
			}
		}

		#endregion
	}
}
