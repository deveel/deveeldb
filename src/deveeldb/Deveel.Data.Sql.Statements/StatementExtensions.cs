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
