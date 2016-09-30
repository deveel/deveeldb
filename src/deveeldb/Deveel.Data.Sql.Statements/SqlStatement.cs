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
using System.Diagnostics;
using System.Linq;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Expressions;

using System.Text;

using Deveel.Data.Diagnostics;
using Deveel.Data.Security;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Statements {
	/// <summary>
	/// Represents the foundation class of SQL statements to be executed.
	/// </summary>
	[Serializable]
	[DebuggerDisplay("{ToString()}")]
	public abstract class SqlStatement : IPreparable, ISerializable, ISqlFormattable, IResourceAccess, IGrantAccess {
		private IList<ResourceAccessRequest> resourceAccess;
		private IList<ResourceGrantRequest> resourceGrant;

		protected SqlStatement() {
			resourceAccess = new List<ResourceAccessRequest>();
			resourceGrant = new List<ResourceGrantRequest>();
		}

		protected SqlStatement(SerializationInfo info, StreamingContext context) {
			SourceQuery = (SqlQuery) info.GetValue("SourceQuery", typeof(SqlQuery));
			IsFromQuery = info.GetBoolean("IsFromQuery");

			var accessRequests = (ResourceAccessRequest[]) info.GetValue("ResourceAccess", typeof(ResourceAccessRequest[]));
			if (accessRequests != null) {
				resourceAccess = new List<ResourceAccessRequest>(accessRequests);
			} else {
				resourceAccess = new List<ResourceAccessRequest>();
			}

			var grantRequests = (ResourceGrantRequest[]) info.GetValue("ResourceGrant", typeof(ResourceGrantRequest[]));
			if (grantRequests != null) {
				resourceGrant = new List<ResourceGrantRequest>(grantRequests);
			} else {
				resourceGrant = new List<ResourceGrantRequest>();
			}
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

		public SqlStatement Parent { get; internal set; }

		IEnumerable<ResourceAccessRequest> IResourceAccess.AccessRequests {
			get { return resourceAccess.AsEnumerable(); }
		}

		IEnumerable<ResourceGrantRequest> IGrantAccess.GrantRequests {
			get { return resourceGrant.AsEnumerable(); }
		}

		internal void Execute(ExecutionContext context) {
			try {
				ConfigureSecurity(context);
				OnBeforeExecute(context);

				ExecuteStatement(context);

				OnAfterExecute(context);
			} catch (ErrorException ex) {
				context.Request.OnError(ex);
				throw;
			} catch (Exception ex) {
				context.Request.OnError(ex);
				throw new StatementException("Statement execution caused an error", ex);
			}
		}

		protected virtual void ConfigureSecurity(ExecutionContext context) {
			context.Assertions.Add(Assert);
		}

		protected virtual void AssertSecurity(ExecutionContext context) {
			var result = context.Assert();
			if (result.IsDenied)
				throw result.SecurityError;
		}

		protected virtual void OnBeforeExecute(ExecutionContext context) {
			AssertSecurity(context);

			context.Request.OnEvent(new StatementEvent(this, StatementEventType.BeforeExecute));
		}

		protected virtual void OnAfterExecute(ExecutionContext context) {
			GrantAccess(context);
			context.Request.OnEvent(new StatementEvent(this, StatementEventType.AfterExecute));
		}

		private void GrantAccess(ExecutionContext context) {
			this.Grant(context);
		}

		protected virtual void ExecuteStatement(ExecutionContext context) {
			throw new NotSupportedException(String.Format("The statement '{0}' does not support execution", GetType().Name));
		}

		private void Assert(ISecurityContext context) {
			try {
				var result = (this as IResourceAccess).Assert(context);
				if (!result.IsAllowed) {
					var error = result.Error;
					if (error is SecurityException)
						throw error;
					throw new SecurityException(
						String.Format("Assertion to resource access from statement '{0}' failed because of an error.", GetType().Name),
						error);
				}
			} catch (SecurityException) {
				throw;
			} catch (Exception ex) {
				throw new SecurityException("An error occurred while asserting the security state.", ex);
			}
		}

		internal void SetSource(SqlQuery query) {
			SourceQuery = query;
			IsFromQuery = true;
		}

		protected void RequestAccess(ObjectName resource, DbObjectType objectType, Privileges privileges) {
			resourceAccess.Add(new ResourceAccessRequest(resource, objectType, privileges));
		}

		protected void RequestDrop(ObjectName resource, DbObjectType objectType) {
			RequestAccess(resource, objectType, Privileges.Drop);
		}

		protected void RequestCreate(ObjectName resource, DbObjectType objectType) {
			RequestAccess(resource, objectType, Privileges.Create);
		}

		protected void RequestAlter(ObjectName resource, DbObjectType objectType) {
			RequestAccess(resource, objectType, Privileges.Alter);
		}

		protected void RequestReference(ObjectName resource, DbObjectType resourceType) {
			RequestAccess(resource, resourceType, Privileges.References);
		}

		protected void RequestSelect(ObjectName resource) {
			RequestAccess(resource, DbObjectType.Table, Privileges.Select);
		}

		protected void RequestSelect(IQueryPlanNode queryPlan) {
			var tables = queryPlan.DiscoverTableNames();
			foreach (var table in tables) {
				RequestSelect(table);
			}
		}

		protected void RequestDelete(ObjectName resource) {
			RequestAccess(resource, DbObjectType.Table, Privileges.Delete);
		}

		protected void RequestInsert(ObjectName resource) {
			RequestAccess(resource, DbObjectType.Table, Privileges.Insert);
		}

		protected void RequestUpdate(ObjectName resource) {
			RequestAccess(resource, DbObjectType.Table, Privileges.Update);
		}

		protected void RequestExecute(ObjectName resource) {
			RequestAccess(resource, DbObjectType.Routine, Privileges.Execute);
		}

		protected void GrantAccess(ObjectName resource, DbObjectType resourceType, Privileges privileges) {
			resourceGrant.Add(new ResourceGrantRequest(resource, resourceType, privileges));
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("SourceQuery", SourceQuery);
			info.AddValue("IsFromQuery", IsFromQuery);
			info.AddValue("ResourceAccess", resourceAccess.ToArray());
			info.AddValue("ResourceGrant", resourceGrant.ToArray());

			GetData(info);
		}

		protected virtual void GetData(SerializationInfo info) {
			
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			return PrepareExpressions(preparer);
		}

		protected virtual SqlStatement PrepareExpressions(IExpressionPreparer preparer) {
			return this;
		}


		protected virtual SqlStatement PrepareStatement(IRequest context) {
			return this;
		}

		internal SqlStatement Prepare(IExpressionPreparer preparer) {
			return PrepareExpressions(preparer);
		}

		internal SqlStatement Prepare(IRequest request) {
			return PrepareStatement(request);
		}

		internal SqlStatement Prepare(IRequest context, IExpressionPreparer preparer) {
			try {
				context.OnEvent(new StatementEvent(this, StatementEventType.BeforePrepare));

				var prepared = PrepareExpressions(preparer);
				if (prepared == null)
					throw new StatementException(String.Format("The statement '{0}' prepared expressions to null", GetType()));

				prepared = prepared.PrepareStatement(context);

				context.OnEvent(new StatementEvent(this, StatementEventType.AfterPrepare));

				return prepared;
			} catch (ErrorException ex) {
				context.OnError(ex);
				throw;
			} catch (Exception ex) {
				context.OnError(ex);
				throw new StatementException("Preparation of the statement caused an error.", ex);
			}
		}

		/// <summary>
		/// Parses a given string into one of more statements.
		/// </summary>
		/// <param name="sqlSource">The input string to be parsed.</param>
		/// <returns>
		/// Returns a list of <see cref="SqlStatement"/> objects resulting from
		/// the parsing of the input string.
		/// </returns>
		/// <exception cref="FormatException">
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
		/// <exception cref="FormatException">
		/// Thrown if the input string is of an invalid format and cannot form
		/// into a valid statement.
		/// </exception>
		public static IEnumerable<SqlStatement> Parse(IContext context, string sqlSource) {
			return Parse(context, new SqlQuery(sqlSource));
		}
		

		public static IEnumerable<SqlStatement> Parse(IContext context, SqlQuery query) {
			if (query == null)
				throw new ArgumentNullException("query");

			ISqlCompiler compiler = null;

			if (context != null) {
				compiler = context.ResolveService<ISqlCompiler>();
			}

			if (compiler == null)
				compiler = new PlSqlCompiler();

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

					throw new FormatException(messages.ToString());
				}

				var statements = result.Statements;

				foreach (var statement in statements) {
					if (statement != null)
						statement.SetSource(query);
				}

				return statements;
			} catch (Exception ex) {
				var messages = new StringBuilder();
				messages.AppendFormat ("The input string '{0}'" + Environment.NewLine, query.Text);
				messages.AppendFormat (" cannot be parsed into SQL Statements, because of {0}" + Environment.NewLine, ex.ToString());
				throw new FormatException(messages.ToString(), ex);
			}
		}

		public override string ToString() {
			var builder = new SqlStringBuilder();
			AppendTo(builder);
			return builder.ToString();
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			AppendTo(builder);
		}

		protected virtual void AppendTo(SqlStringBuilder builder) {
			
		}
	}
}