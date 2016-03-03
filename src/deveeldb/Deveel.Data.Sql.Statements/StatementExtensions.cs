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

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public static class StatementExtensions {
		public static IStatement Prepare(this IStatement statement, IExpressionPreparer preparer) {
			if (statement is IPreparable)
				statement = ((IPreparable) statement).Prepare(preparer) as IStatement;

			return statement;
		}

		public static IStatement PrepareStatement(this IStatement statement, IRequest context) {
			if (statement is IPreparableStatement)
				statement = ((IPreparableStatement) statement).Prepare(context);

			return statement;
		}

		/// <summary>
		/// Prepares this statement and returns an object that can be executed
		/// within a given context.
		/// </summary>
		/// <param name="statement"></param>
		/// <param name="preparer">An object used to prepare the expressions contained in the statement.</param>
		/// <param name="context">The execution context used to prepare the statement properties.</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlStatement"/> that represents the
		/// prepared version of this statement and that will be executed in a later moment.
		/// </returns>
		/// <exception cref="StatementPrepareException">
		/// Thrown if an error occurred while preparing the statement.
		/// </exception>
		public static IStatement Prepare(this IStatement statement, IExpressionPreparer preparer, IRequest context) {
			if (statement is IPreparable)
				statement = ((IPreparable) statement).Prepare(preparer) as IStatement;

			if (statement == null)
				return null;

			if (statement is IPreparableStatement)
				statement = ((IPreparableStatement) statement).Prepare(context);

			return statement;
		}
	}
}
