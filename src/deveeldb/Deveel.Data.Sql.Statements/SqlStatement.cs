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
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// Represents the foundation class of SQL statements to be executed.
	/// </summary>
	/// <remarks>
	/// <para>
	/// A SQL Statement encapsulates the properties that are required to form
	/// a <see cref="SqlPreparedStatement"/> that can be serialized and executed.
	/// </para>
	/// <para>
	/// It is prevented to a <see cref="SqlStatement"/> to be immediately executed
	/// for enforcing <see cref="SqlPreparedStatement"/> to be cached and executed
	/// in later moments, optimizing performances and re-usability.
	/// </para>
	/// </remarks>
	/// <seealso cref="SqlPreparedStatement"/>
	[Serializable]
	public abstract class SqlStatement {
		/// <summary>
		/// Gets the <see cref="SqlQuery"/> that is the origin of this statement.
		/// </summary>
		/// <remarks>
		/// A single SQL query can form multiple <see cref="SqlStatement"/> objects.
		/// </remarks>
		/// <seealso cref="SqlQuery"/>
		public SqlQuery SourceQuery { get; private set; }

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

		/// <summary>
		/// Gets the type of this SQL statement 
		/// </summary>
		/// <seealso cref="StatementType"/>
		public abstract StatementType StatementType { get; }

		/// <summary>
		/// When overridden by an implementing class, this method generates a prepared
		/// version of this statement that can be executed.
		/// </summary>
		/// <param name="preparer">An object used to prepare the SQL expressions contained
		/// into the statement.</param>
		/// <param name="context">The executing context in used to prepare the statement
		/// properties.</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlPreparedStatement"/> that represents the
		/// prepared version of this statement and that will be executed in a later moment.
		/// </returns>
		/// <seealso cref="SqlPreparedStatement"/>
		/// <seealso cref="Prepare(IExpressionPreparer, IQueryContext)"/>
		protected abstract SqlPreparedStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context);

		/// <summary>
		/// Prepares this statement and returns an object that can be executed
		/// within a given context.
		/// </summary>
		/// <param name="context">The execution context used to prepare the statement properties.</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlPreparedStatement"/> that represents the
		/// prepared version of this statement and that will be executed in a later moment.
		/// </returns>
		/// <exception cref="StatementPrepareException">
		/// Thrown if an error occurred while preparing the statement.
		/// </exception>
		public SqlPreparedStatement Prepare(IQueryContext context) {
			return Prepare(null, context);
		}

		/// <summary>
		/// Prepares this statement and returns an object that can be executed
		/// within a given context.
		/// </summary>
		/// <param name="preparer">An object used to prepare the expressions contained in the statement.</param>
		/// <param name="context">The execution context used to prepare the statement properties.</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlPreparedStatement"/> that represents the
		/// prepared version of this statement and that will be executed in a later moment.
		/// </returns>
		/// <exception cref="StatementPrepareException">
		/// Thrown if an error occurred while preparing the statement.
		/// </exception>
		public SqlPreparedStatement Prepare(IExpressionPreparer preparer, IQueryContext context) {
			SqlPreparedStatement prepared;

			try {
				prepared = PrepareStatement(preparer, context);

				if (prepared == null)
					throw new InvalidOperationException("Preparation was invalid.");

				prepared.Source = this;
			} catch(StatementPrepareException) {
				throw;
			} catch (Exception ex) {
				throw new StatementPrepareException(String.Format("An error occurred while preparing a statement of type '{0}'.", StatementType), ex);
			}

			return prepared;
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
		public ITable Evaluate(IQueryContext context) {
			return Evaluate(null, context);
		}

		/// <summary>
		/// Prepares and evaluates this statement into a tabular result.
		/// </summary>
		/// <param name="preparer">An object used to prepare the SQL expressions contained
		/// in the statement before the execution.</param>
		/// <param name="context">The context used to prepare and evaluate the statement.</param>
		/// <returns>
		/// Returns a <see cref="ITable"/> object that contains the values resulting from the
		/// evaluation of the statement.
		/// </returns>
		/// <exception cref="StatementPrepareException">
		/// Thrown if an error occurred while preparing the statement.
		/// </exception>
		public ITable Evaluate(IExpressionPreparer preparer, IQueryContext context) {
			SqlPreparedStatement prepared;

			try {
				prepared = Prepare(preparer, context);
			} catch (Exception ex) {
				throw new InvalidOperationException("Unable to prepare the statement for execution.", ex);
			}

			return prepared.Evaluate(context);
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
			return Parse(new SqlQuery(sqlSource));
		}

		public static IEnumerable<SqlStatement> Parse(SqlQuery query) {
			if (query == null)
				throw new ArgumentNullException("query");

			var compiler = SqlParsers.Default;

			try {
				var sqlSource = query.Text;
				var result = compiler.Parse(query.Text);
				if (result.HasErrors)
					throw new SqlParseException();

				var builder = new StatementBuilder();
				var statements = builder.Build(result.RootNode, sqlSource).ToList();

				foreach (var statement in statements) {
					statement.IsFromQuery = true;
					statement.SourceQuery = query;
				}

				return statements;
			} catch (SqlParseException) {
				throw;
			} catch (Exception ex) {
				throw new SqlParseException("The input string cannot be parsed into SQL Statements", ex);
			}
		}

		public override string ToString() {
			var visitor = new StatementSqlStringBuilder();
			visitor.Visit(this);
			return visitor.ToString();
		}
	}
}