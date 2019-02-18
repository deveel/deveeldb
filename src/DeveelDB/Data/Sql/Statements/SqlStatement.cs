// 
//  Copyright 2010-2018 Deveel
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
using System.Reflection;
using System.Threading.Tasks;

using Deveel.Data.Diagnostics;
using Deveel.Data.Events;
using Deveel.Data.Security;
using Deveel.Data.Services;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements.Security;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// The base class for the definition of SQL statements that are
	/// interpreted by the system
	/// </summary>
	/// <remarks>
	/// <para>
	/// SQL statements are the single tasks that are executed against
	/// a system and they are composed by expressions and references.
	/// </para>
	/// </remarks>
	public abstract class SqlStatement : ISqlFormattable, ISqlExpressionPreparable<SqlStatement>, IEventSource {
		private IDictionary<string, object> metadata;

		protected SqlStatement() {
		}

		/// <summary>
		/// Gets a boolean value indicating if this statement is preparable
		/// for execution.
		/// </summary>
		/// <seealso cref="Prepare(IContext)"/>
		public virtual bool CanPrepare => true;

		/// <summary>
		/// Gets a descriptive name of the SQL statement
		/// </summary>
		protected virtual string Name {
			get {
				var name = GetType().Name;
				if (name.EndsWith("Statenment", StringComparison.OrdinalIgnoreCase))
					name = name.Substring(0, name.Length - 10);

				return name;
			}
		}

		/// <summary>
		/// Gets the location of the statement within a context
		/// </summary>
		/// <remarks>
		/// <para>
		/// Complex execution contexts, such as blocks, functions and procedures,
		/// are composed by multiple statements: the location is relative to the 
		/// textual occurrence of the beginning of the statement within that
		/// context, identified by the starting line and starting column.
		/// </para>
		/// </remarks>
		/// <seealso cref="LocationInfo"/>
		public LocationInfo Location { get; set; }

		internal SqlStatement Parent { get; set; }

		IDictionary<string, object> IEventSource.Metadata {
			get {
				if (metadata == null) {
					metadata = new Dictionary<string, object>();
					CollectMetadata(metadata);
				}

				return metadata;
			}
		}

		// TODO:
		IEventSource IEventSource.ParentSource => null;

		/// <summary>
		/// Gets the previous statement in an execution context
		/// </summary>
		/// <seealso cref="Next"/>
		public SqlStatement Previous { get; internal set; }

		/// <summary>
		/// Gets the next statement in an execution context
		/// </summary>
		/// <seealso cref="Previous"/>
		public SqlStatement Next { get; internal set; }

		/// <summary>
		/// Creates a context that can be used to execute the statement
		/// against the underlying system
		/// </summary>
		/// <param name="parent">The parent context of the execution</param>
		/// <returns>
		/// Returns an instance of <see cref="StatementContext"/> that represents
		/// the context used for the execution of the statement, inheriting from
		/// a given parent.
		/// </returns>
		protected virtual StatementContext CreateContext(IContext parent) {
			return new StatementContext(parent, this);
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			AppendTo(builder);
		}

		protected virtual void AppendTo(SqlStringBuilder builder) {
			
		}

		private void CollectMetadata(IDictionary<string, object> data) {
			var meta = new Dictionary<string, object>();

			if (Location != null) {
				data["statement.line"] = Location.Line;
				data["statement.column"] = Location.Column;
			}

			data["statement.sql"] = this.ToSqlString();

			GetMetadata(meta);

			foreach (var pair in meta) {
				var key = pair.Key;
				if (!key.StartsWith("statement.", StringComparison.OrdinalIgnoreCase))
					key = $"statement.{key}";

				data[key] = pair.Value;
			}
		}

		/// <summary>
		/// Collects metadata about the statement that are useful for
		/// diagnostics and reporting
		/// </summary>
		/// <param name="data">The key/value dictionary that holds the metadata for
		/// this statement.</param>
		protected virtual void GetMetadata(IDictionary<string, object> data) {
			
		}

		SqlStatement ISqlExpressionPreparable<SqlStatement>.Prepare(ISqlExpressionPreparer preparer) {
			return PrepareExpressions(preparer);
		}

		protected virtual SqlStatement PrepareExpressions(ISqlExpressionPreparer preparer) {
			return this;
		}

		protected virtual Task<SqlStatement> PrepareStatementAsync(IContext context) {
			return Task.FromResult(this);
		}

		protected virtual void Require(IRequirementCollection requirements) {
		}

		/// <summary>
		/// Prepares the statement for execution against a given context
		/// </summary>
		/// <param name="context">The context used to prepare the statement</param>
		/// <remarks>
		/// <para>
		/// When SQL statements are defined they are still in an unresolved mode:
		/// this means that any reference to resources is not resolved, expressions
		/// are not reduced or projected. Invoking this method would create an
		/// instance of <see cref="SqlStatement"/> that is optimal for execution.
		/// </para>
		/// <para>
		/// In the context of storing a method (function or procedure), this method
		/// is invoked before the storage, to optimize the execution to subsequent 
		/// calls to the stoed method.
		/// </para>
		/// <para>
		/// <strong>Note:</strong> It is not assured that the result instance of <see cref="SqlStatement"/>
		/// returned from this method will be of the same type of statement invoked.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="SqlStatement"/> that is ready to be executed
		/// by the system.
		/// </returns>
		/// <exception cref="SqlStatementException">If an error occurred while preparing
		/// the statement.</exception>
		/// <seealso cref="CanPrepare"/>
		public async Task<SqlStatement> PrepareAsync(IContext context) {
			using (var statementContext = CreateContext(context)) {
				var preparers = (statementContext as IContext).Scope.ResolveAll<ISqlExpressionPreparer>();
				var result = this;

				foreach (var preparer in preparers) {
					result = PrepareExpressions(preparer);
				}

				if (CanPrepare)
					result = await result.PrepareStatementAsync(statementContext);

				return result;
			}
		}


		private async Task CheckRequirements(IContext context) {
			//TODO: context.Debug(-1, "Collecting security requirements");

			var registry = new RequirementCollection();
			Require(registry);

			//TODO: context.Debug(-1, "Check security requirements");

			try {
				foreach (var requirement in registry) {
					await requirement.HandleRequirementAsync(context);
				}
			}
			catch (UnauthorizedAccessException) {
				//TODO: context.Error(-93884, $"User {context.User().Name} has not enough rights to execute", ex);
				throw;
			}
			catch (Exception) {
				//TODO: context.Error(-83993, "Unknown error while checking requirements", ex);
				throw;
			}
		}

		/// <summary>
		/// Executes the statement against the given context
		/// </summary>
		/// <param name="context">The context used by the statement to operate
		/// against the underlying system.</param>
		/// <remarks>
		/// <para>
		/// A statement is a task that must be executed for its effects to take
		/// effect against the underlying system.
		/// </para>
		/// <para>
		/// Before proceeding to the execution of the statement, this method
		/// assesses the privileges of the invoking user, throwing an exception
		/// if unhauthorized to execute this statement, child statements or to
		/// access any referenced resource in the tree. For example, a user can
		/// be authorized to execute a <c>SELECT</c> statement from a given set
		/// of tables but not from other tables in the statement, and this will 
		/// trigger an exception. 
		/// </para>
		/// <para>
		/// The result of this method is polimorphic depending on the kind
		/// of statement executed: for example executing a command statement will 
		/// return a reference to the cursor opened, while the selection of
		/// single value (from a table, a function or a variable) will return
		/// a constant result.
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="IStatementResult"/> that describes the
		/// result of the exeuction of the statement.
		/// </returns>
		/// <exception cref="SqlStatementException">If an unhandled error occurred
		/// while executing the statement.</exception>
		/// <exception cref="UnauthorizedAccessException">If the user owner of the
		/// context is not authorized to execute the statement, or if has not the
		/// rights to access any referenced resource.</exception>
		/// <seealso cref="ExecuteStatementAsync"/>
		public async Task<IStatementResult> ExecuteAsync(IContext context) {
			using (var statementContext = CreateContext(context)) {
				//TODO: statementContext.Information(201, "Executing statement");

				await CheckRequirements(statementContext);

				try {
					await ExecuteStatementAsync(statementContext);
					return statementContext.Result;
				} catch (SqlStatementException) {
					//TODO: statementContext.Error(-670393, "The statement thrown an error", ex);
					throw;
				} catch (Exception ex) {
					//TODO: statementContext.Error(-1, "Could not execute the statement", ex);
					throw new SqlStatementException("Could not execute the statement because of an error", ex);
				} finally {
					//TODO: statementContext.Information(202, "The statement was executed");
				}
			}
		}

		/// <summary>
		/// When overridden by an inheriting class, executes the body
		/// of the statement against the given context.
		/// </summary>
		/// <param name="context">The context used by the statement to operate
		/// against the underlying system.</param>
		/// <remarks>
		/// <para>
		/// This method represents the core execution of the statement,
		/// after the assessments of the authorizations of the user
		/// executing have been done.
		/// </para>
		/// <para>
		/// Any exception thrown by this method will be catch on upper level
		/// by <see cref="ExecuteAsync"/>.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		protected abstract Task ExecuteStatementAsync(StatementContext context);

		public override string ToString() {
			return this.ToSqlString();
		}
	}
}