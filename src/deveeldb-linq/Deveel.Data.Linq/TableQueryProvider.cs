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
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using Deveel.Data.Sql;

namespace Deveel.Data.Linq {
	class TableQueryProvider : IQueryProvider {
		public TableQueryProvider(ITable table) {
			if (table == null)
				throw new ArgumentNullException("table");

			Table = table;
		}

		public ITable Table { get; private set; }

		public IQueryable CreateQuery(Expression expression) {
			var elementType = TypeSystem.GetElementType(expression.Type);

			try {
				var type = typeof (QueryableTable<>).MakeGenericType(elementType);
				return (IQueryable) Activator.CreateInstance(type, new object[] {this, expression});
			} catch (TargetInvocationException ex) {
				throw ex.InnerException;
			}
		}

		public IQueryable<TElement> CreateQuery<TElement>(Expression expression) {
			return new QueryableTable<TElement>(this, expression);
		}

		public object Execute(Expression expression) {
			return Execute(expression, false);
		}


		private object Execute(Expression expression, bool isEnum) {
			// The expression must represent a query over the data source.
			if (!IsQueryOverDataSource(expression))
				throw new InvalidProgramException("No query over the data source was specified.");

			var elementType = TypeSystem.GetElementType(expression.Type);

			IQueryable queryResult;

			var builder = new QueryBuilder();

			// Find the call to Where() and get the lambda expression predicate.
			var whereFinder = new InnermostWhereFinder();
			var whereExpression = whereFinder.GetInnermostWhere(expression);

			if (whereExpression == null) {
				var query = builder.Build();
				queryResult = query.Execute(elementType, Table).AsQueryable();
			} else {
				var lambdaExpression = (LambdaExpression)((UnaryExpression)(whereExpression.Arguments[1])).Operand;

				// Send the lambda expression through the partial evaluator.
				lambdaExpression = (LambdaExpression)Evaluator.PartialEval(lambdaExpression);

				var query = builder.Build(lambdaExpression.Body);
				queryResult = query.Execute(elementType, Table).AsQueryable();
			}

			// Copy the expression tree that was passed in, changing only the first
			// argument of the innermost MethodCallExpression.
			var treeCopier = new ExpressionTreeModifier(queryResult, elementType);
			var newExpressionTree = treeCopier.Visit(expression);

			// This step creates an IQueryable that executes by replacing Queryable methods with Enumerable methods.
			if (isEnum)
				return queryResult.Provider.CreateQuery(newExpressionTree);
			
			return queryResult.Provider.Execute(newExpressionTree);
		}

		private static bool IsQueryOverDataSource(Expression expression) {
			// If expression represents an unqueried IQueryable data source instance,
			// expression is of type ConstantExpression, not MethodCallExpression.
			return (expression is MethodCallExpression);
		}

		public TResult Execute<TResult>(Expression expression) {
			bool isEnum = (typeof(TResult).Name == "IEnumerable`1");
			return (TResult) Execute(expression, isEnum);
		}

		public string GetQueryText(Expression expression) {
			throw new NotImplementedException();
		}
	}
}
