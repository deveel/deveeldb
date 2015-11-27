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

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Parser;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// Represents the foundation class of SQL statements to be executed.
	/// </summary>
	[Serializable]
	public abstract class SqlStatement : IStatement, ISerializable {
		protected SqlStatement() {
			
		}

		protected SqlStatement(ObjectData data) {
			SourceQuery = data.GetValue<SqlQuery>("SourceQuery");
			IsFromQuery = data.GetBoolean("IsFromQuery");
		}

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

		void ISerializable.GetData(SerializeData data) {
			data.SetValue("SourceQuery", SourceQuery);
			data.SetValue("IsFromQuery", IsFromQuery);

			GetData(data);
		}

		protected virtual void GetData(SerializeData data) {
			
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
		public IExecutable Prepare(IExpressionPreparer preparer, IRequest context) {
			IStatement prepared = this;

			try {
				if (preparer != null && prepared is IPreparable)
					prepared = ((IPreparable)this).Prepare(preparer) as IStatement;

				if (context != null && prepared is IPreparableStatement)
					prepared = ((IPreparableStatement)prepared).Prepare(context);

				if (prepared == null)
					throw new InvalidOperationException("Unable to prepare the statement.");
			} catch(StatementPrepareException) {
				throw;
			} catch (Exception ex) {
				throw new StatementPrepareException(String.Format("An error occurred while preparing a statement of type '{0}'.", GetType()), ex);
			}

			return prepared as IExecutable;
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
		public ITable Execute(IRequest context) {
			return PrepareAndExecute(null, context);
		}

		private ITable PrepareAndExecute(IExpressionPreparer preparer, IRequest context) {
			IExecutable prepared;

			try {
				prepared = Prepare(preparer, context);
			} catch (Exception ex) {
				throw new InvalidOperationException("Unable to prepare the statement for execution.", ex);
			}

			if (prepared == null)
				throw new InvalidOperationException();

			var exeContext = new ExecutionContext(context);
			prepared.Execute(exeContext);
			if (!exeContext.HasResult)
				return FunctionTable.ResultTable(context, 0);

			return exeContext.Result;
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
		public static IEnumerable<SqlStatement> Parse(IContext context, string sqlSource) {
			return Parse(context, new SqlQuery(sqlSource));
		}

		private static readonly ISqlCompiler DefaultCompiler = new SqlDefaultCompiler();

		public static IEnumerable<SqlStatement> Parse(IContext context, SqlQuery query) {
			if (query == null)
				throw new ArgumentNullException("query");

			var compiler = DefaultCompiler;

			if (context != null) {
				compiler = context.ResolveService<ISqlCompiler>();
			}

			try {
				var compileContext = new SqlCompileContext(context, query.Text);
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