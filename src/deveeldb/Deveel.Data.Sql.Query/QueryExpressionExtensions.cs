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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Query {
	static class QueryExpressionExtensions {
		public static void DiscoverAccessedResources(this SqlExpression expression, IDictionary<ObjectName, QueryAccessedResource> tableNames) {
			var visitor = new TableNamesVisitor(tableNames);
			visitor.Visit(expression);
		}

		public static IList<QueryReference> DiscoverQueryReferences(this SqlExpression expression, ref int level) {
			return DiscoverQueryReferences(expression, ref level, new List<QueryReference>());
		}

		public static IList<QueryReference> DiscoverQueryReferences(this SqlExpression expression, ref int level, IList<QueryReference> list) {
			var visitor = new QueryReferencesVisitor(list, level);
			visitor.Visit(expression);
			level = visitor.Level;
			return visitor.References;
		}

		public static IEnumerable<ObjectName> DiscoverReferences(this SqlExpression expression) {
			var discovery = new ReferenceDiscovery();
			return discovery.Discover(expression);
		}

		public static bool HasSubQuery(this SqlExpression expression) {
			return new SubQueryDiscovery().Verify(expression);
		}

		#region ColumnNameDiscovery

		class ReferenceDiscovery : SqlExpressionVisitor {
			private readonly List<ObjectName> columnNames;

			public ReferenceDiscovery() {
				columnNames = new List<ObjectName>();
			}

			public IEnumerable<ObjectName> Discover(SqlExpression expression) {
				Visit(expression);
				return columnNames.ToArray();
			}

			public override SqlExpression VisitConstant(SqlConstantExpression constant) {
				var value = constant.Value;
				if (value.Type is ArrayType) {
					var array = (SqlArray) value.Value;
					foreach (var element in array) {
						columnNames.AddRange(element.DiscoverReferences());
					}
				}

				return base.VisitConstant(constant);
			}

			public override SqlExpression VisitFunctionCall(SqlFunctionCallExpression expression) {
				var args = expression.Arguments;
				foreach (var arg in args) {
					columnNames.AddRange(arg.Value.DiscoverReferences());
				}

				return base.VisitFunctionCall(expression);
			}

			public override SqlExpression VisitReference(SqlReferenceExpression reference) {
				columnNames.Add(reference.ReferenceName);
				return base.VisitReference(reference);
			}
		}

		#endregion

		#region SubQueryDiscovery

		private class SubQueryDiscovery : SqlExpressionVisitor {
			private bool hasSubQuery;

			public bool Verify(SqlExpression expression) {
				Visit(expression);
				return hasSubQuery;
			}

			public override SqlExpression VisitConstant(SqlConstantExpression constant) {
				if (constant.Value.Type is QueryType)
					hasSubQuery = true;

				return base.VisitConstant(constant);
			}
		}

		#endregion
	}
}
