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
using System.Runtime.Serialization;

using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Parser;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;
using System.Text;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// Represents the foundation class of SQL statements to be executed.
	/// </summary>
	[Serializable]
	public abstract class SqlStatement : IStatement, IExecutable, ISerializable {
		protected SqlStatement() {
			
		}

		protected SqlStatement(SerializationInfo info, StreamingContext context) {
			SourceQuery = (SqlQuery) info.GetValue("SourceQuery", typeof(SqlQuery));
			IsFromQuery = info.GetBoolean("IsFromQuery");
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

		void IExecutable.Execute(ExecutionContext context) {
			ExecuteStatement(context);
		}

		protected virtual void ExecuteStatement(ExecutionContext context) {
			throw new NotSupportedException(String.Format("The statement '{0}' does not support execution", GetType().Name));
		}

		internal void SetSource(SqlQuery query) {
			SourceQuery = query;
			IsFromQuery = true;
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("SourceQuery", SourceQuery);
			info.AddValue("IsFromQuery", IsFromQuery);

			GetData(info, context);
		}

		protected virtual void GetData(SerializationInfo info, StreamingContext context) {
			
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
				prepared = this.Prepare(preparer, context) as IExecutable;
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
				{
					var messages = new StringBuilder();
					messages.AppendFormat("SqlParseException for '{0}'" + Environment.NewLine, query.Text);
					foreach (var m in result.Messages)
					{
						messages.AppendFormat("Level: {0}", m.Level);
						if (null != m.Location)
						{
							messages.AppendFormat(", Line: {0}, Column: {1}", m.Location.Line, m.Location.Column);
						}
						messages.AppendFormat(", Message: {0}", m.Text);
						messages.AppendLine();
					}
					throw new SqlParseException(messages.ToString());
				}

				var statements = result.CodeObjects.Cast<SqlStatement>();

				foreach (var statement in statements) {
					if (statement != null)
						statement.SetSource(query);
				}

				return statements;
			} catch (SqlParseException) {
				throw;
			} catch (Exception ex) {
				var messages = new StringBuilder();
				messages.AppendFormat ("The input string '{0}'" + Environment.NewLine, query.Text);
				messages.AppendFormat (" cannot be parsed into SQL Statements, because of {0}" + Environment.NewLine, ex.ToString());
				throw new SqlParseException(messages.ToString(), ex);
			}
		}
	}
}