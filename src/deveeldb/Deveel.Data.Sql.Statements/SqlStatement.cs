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

using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Parser;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// Represents the foundation class of SQL statements to be executed.
	/// </summary>
	public abstract class SqlStatement : IStatement {
		/// <summary>
		/// Gets the <see cref="SqlQuery"/> that is the origin of this statement.
		/// </summary>
		/// <remarks>
		/// A single SQL query can form multiple <see cref="SqlStatement"/> objects.
		/// </remarks>
		/// <seealso cref="SqlQuery"/>
		public SqlQuery SourceQuery { get; set; }

		/// <summary>
		/// Gets a boolean value indicating if this object was formed from the parsing
		/// of a <see cref="SqlQuery"/> or if it was manually created.
		/// </summary>
		/// <remarks>
		/// When this value is <c>true</c> the value returned by <see cref="SourceQuery"/>
		/// is not <c>null</c>.
		/// </remarks>
		/// <seealso cref="SourceQuery"/>
		public bool IsFromQuery { get; private set; }

		internal void SetSource(SqlQuery query) {
			SourceQuery = query;
			IsFromQuery = true;
		}

		protected virtual bool IsPreparable {
			get { return true; }
		}

		/// <summary>
		/// When overridden by an implementing class, this method generates a prepared
		/// version of this statement that can be executed.
		/// </summary>
		/// <param name="context">The executing context in used to prepare the statement
		/// properties.</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlStatement"/> that represents the
		/// prepared version of this statement and that will be executed in a later moment.
		/// </returns>
		/// <seealso cref="Prepare(IExpressionPreparer, IQueryContext)"/>
		protected virtual SqlStatement PrepareStatement(IQueryContext context) {
			return this;
		}

		protected virtual SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			return this;
		}

		/// <summary>
		/// Prepares this statement and returns an object that can be executed
		/// within a given context.
		/// </summary>
		/// <param name="preparer">An object used to prepare the expressions contained in the statement.</param>
		/// <param name="context">The execution context used to prepare the statement properties.</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlStatement"/> that represents the
		/// prepared version of this statement and that will be executed in a later moment.
		/// </returns>
		/// <exception cref="StatementPrepareException">
		/// Thrown if an error occurred while preparing the statement.
		/// </exception>
		public SqlStatement Prepare(IExpressionPreparer preparer, IQueryContext context) {
			SqlStatement prepared = this;

			try {
				if (preparer != null)
					prepared = PrepareExpressions(preparer);

				if (context != null)
					prepared = PrepareStatement(context);

				if (prepared == null)
					throw new InvalidOperationException("Unable to prepare the statement.");
			} catch(StatementPrepareException) {
				throw;
			} catch (Exception ex) {
				throw new StatementPrepareException(String.Format("An error occurred while preparing a statement of type '{0}'.", GetType()), ex);
			}

			return prepared;
		}

		IStatement IStatement.Prepare(IQueryContext context) {
			return PrepareStatement(context);
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return PrepareExpressions(preparer);
		}

		/// <summary>
		/// Prepares and evaluates this statement into a tabular result.
		/// </summary>
		/// <param name="context">The context used to prepare and evaluate the statement.</param>
		/// <returns>
		/// Returns a <see cref="ITable"/> object that contains the values resulting from the
		/// evaluation of the statement.
		/// </returns>
		/// <exception cref="StatementPrepareException">
		/// Thrown if an error occurred while preparing the statement.
		/// </exception>
		public ITable Execute(IQueryContext context) {
			return PrepareAndExecute(null, context);
		}

		ITable IExecutable.Execute(IQueryContext context) {
			return ExecuteStatement(context);
		}

		private ITable PrepareAndExecute(IExpressionPreparer preparer, IQueryContext context) {
			SqlStatement prepared;

			try {
				prepared = Prepare(preparer, context);
			} catch (Exception ex) {
				throw new InvalidOperationException("Unable to prepare the statement for execution.", ex);
			}

			return prepared.ExecuteStatement(context);
		}

		protected virtual ITable ExecuteStatement(IQueryContext context) {
			// This method is not abstract because a statement can be different after
			// preparation, that means a statement can be a builder for another 
			throw new PreparationRequiredException(GetType().FullName);
		}

		/// <summary>
		/// Parses a given string into one of more statements.
		/// </summary>
		/// <param name="sqlSource">The input string to be parsed.</param>
		/// <returns>
		/// Returns a list of <see cref="SqlStatement"/> objects resulting from
		/// the parsing of the input string.
		/// </returns>
		/// <exception cref="SqlParseException">
		/// Thrown if the input string is of an invalid format and cannot form
		/// into a valid statement.
		/// </exception>
		public static IEnumerable<SqlStatement> Parse(string sqlSource) {
			return Parse(null, sqlSource);
		}

		/// <summary>
		/// Parses a given string into one of more statements.
		/// </summary>
		/// <param name="context"></param>
		/// <param name="sqlSource">The input string to be parsed.</param>
		/// <returns>
		/// Returns a list of <see cref="SqlStatement"/> objects resulting from
		/// the parsing of the input string.
		/// </returns>
		/// <exception cref="SqlParseException">
		/// Thrown if the input string is of an invalid format and cannot form
		/// into a valid statement.
		/// </exception>
		public static IEnumerable<SqlStatement> Parse(IQueryContext context, string sqlSource) {
			return Parse(context, new SqlQuery(sqlSource));
		}

		private static readonly ISqlCompiler DefaultCompiler = new SqlDefaultCompiler();

		public static IEnumerable<SqlStatement> Parse(IQueryContext context, SqlQuery query) {
			if (query == null)
				throw new ArgumentNullException("query");

			var compiler = DefaultCompiler;

			ISystemContext systemContext = null;

			if (context != null) {
				systemContext = context.SystemContext();
				compiler = systemContext.SqlCompiler();
			}

			try {
				var compileContext = new SqlCompileContext(systemContext, query.Text);
				var result = compiler.Compile(compileContext);
				if (result.HasErrors)
					throw new SqlParseException();

				var statements = result.Statements;

				foreach (var statement in statements) {
					if (statement != null)
						statement.SetSource(query);
				}

				return statements;
			} catch (SqlParseException) {
				throw;
			} catch (Exception ex) {
				throw new SqlParseException("The input string cannot be parsed into SQL Statements", ex);
			}
		}
	}
}