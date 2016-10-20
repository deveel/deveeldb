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
using System.Linq;

using Deveel.Data.Security;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql {
	public sealed class ExecutionContext : ISecurityContext, IDisposable {
		public ExecutionContext(IRequest request, SqlStatement statement)
			: this(null, request, statement) {
		}

		private ExecutionContext(ExecutionContext parent, IRequest request, SqlStatement statement) {
			if (request == null)
				throw new ArgumentNullException("request");

			Parent = parent;
			Request = request;
			Statement = statement;

			Assertions = new SecurityAssertionRegistrar(parent != null ? parent.Assertions : null);
			Actions = new SecurityActionRegistrar();
		}

		~ExecutionContext() {
			Dispose(false);
		}

		private SqlStatement Statement { get; set; }

		object ISecurityContext.Target {
			get { return Statement; }
		}

		public IRequest Request { get; private set; }

		public ITable Result { get; private set; }

		public bool HasTermination { get; private set; }

		public bool HasResult {
			get { return Result != null; }
		}

		public ICursor Cursor { get; private set; }

		public bool HasCursor { get; private set; }

		public User User {
			get { return Request.Query.User(); }
		}

		public IQuery Query {
			get { return Request.Query; }
		}

		public bool IsInSession {
			get { return Query.IsInSession(); }
		}

		private ExecutionContext Parent { get; set; }

		public SystemAccess DirectAccess {
			get { return Request.Access(); }
		}

		public SecurityActionRegistrar Actions { get; private set; }

		public SecurityAssertionRegistrar Assertions { get; private set; }

		private void AssertNotFinished() {
			if (HasTermination)
				throw new InvalidOperationException("The context has already terminated.");
		}

		private void Terminate() {
			HasTermination = true;

			if (Parent != null) {
				Parent.Cursor = Cursor;
				Parent.HasCursor = HasCursor;
				Parent.Result = Result;
				Parent.Terminate();
			}
		}

		private void Dispose(bool disposing) {
			if (disposing) {
				if (Assertions != null)
					Assertions.Dispose();
				if (Actions != null)
					(Actions as IDisposable).Dispose();
			}

			Statement = null;
			Parent = null;
			Request = null;
			Result = null;
			Cursor = null;
			Assertions = null;
			Actions = null;
		}

		public void Dispose() {
			GC.SuppressFinalize(this);
			Dispose(true);
		}

		IEnumerable<ISecurityAssert> ISecurityContext.Assertions {
			get { return Assertions.Assertions; }
		}

		public void SetResult(ITable result) {
			AssertNotFinished();
			Result = result;
		}

		public void SetResult(int value) {
			if (IsInSession)
				SetResult(FunctionTable.ResultTable(Request, value));
		}

		public void SetResult(Field value) {
			if (IsInSession)
				SetResult(FunctionTable.ResultTable(Request, value));
		}

		public void SetCursor(ICursor cursor) {
			if (cursor == null)
				throw new ArgumentNullException("cursor");

			if (HasResult)
				throw new InvalidOperationException("The context has already a result set.");
			if (HasCursor)
				throw new InvalidOperationException("A cursor was already set as result of this context.");

			Cursor = cursor;
			HasCursor = true;
		}

		internal void Raise(string exceptionName) {
			if (String.IsNullOrEmpty(exceptionName))
				throw new ArgumentNullException("exceptionName");

			AssertNotFinished();

			try {
				var statement = Statement;
				while (statement != null) {
					if (statement is PlSqlBlockStatement) {
						var block = (PlSqlBlockStatement) statement;
						if (block.Handles(exceptionName)) {
							block.FireHandler(this, exceptionName);
							return;
						}
					}

					statement = statement.Parent;
				}

				if (SystemErrorCodes.IsSystemError(exceptionName)) {
					var errorCode = SystemErrorCodes.GetErrorCode(exceptionName);
					throw new StatementException(errorCode, String.Format("Exception '{0}' explicitly risen from code", exceptionName));
				}

				var declared = Request.Context.FindDeclaredException(exceptionName);
				if (declared == null)
					throw new InvalidOperationException(String.Format("Exception '{0}' was not declared in the context.", exceptionName));

				throw new StatementException(declared.ErrorCode,
					String.Format("Declared exception '{0}' explicitly risen from code", exceptionName));
			} finally {
				Terminate();
			}
		}

		internal void Control(LoopControlType controlType, string label) {
			AssertNotFinished();

			bool controlled = false;

			var statement = Statement;
			while (statement != null) {
				if (statement is LoopStatement) {
					var loop = (LoopStatement) statement;
					if (!String.IsNullOrEmpty(label) &&
					    String.Equals(label, loop.Label)) {
						loop.Control(controlType);
						controlled = true;
					} else if (!controlled) {
						loop.Control(controlType);
						controlled = true;
					}
				}

				statement = statement.Parent;
			}

			if (!controlled)
				throw new StatementException(String.Format("Could not control {0} any loop.",
					controlType.ToString().ToUpperInvariant()));
		}

		internal void Transfer(string label) {
			AssertNotFinished();

			if (String.IsNullOrEmpty(label))
				throw new ArgumentNullException("label");

			var statement = FindInTree(Statement, label);
			if (statement == null)
				throw new StatementException(String.Format("Could not find any block labeled '{0}' in the execution tree.", label));

			var block = NewBlock(statement);
			statement.Execute(block);

			if (block.HasResult)
				SetResult(block.Result);
		}

		internal void Return(SqlExpression value) {
			AssertNotFinished();

			if (value != null)
				SetResult(FunctionTable.ResultTable(Request, value));

			Terminate();
		}

		internal void BeforeExecute() {
			Actions.BeforeExecute(this);
		}

		internal void AfterExecute() {
			Actions.AfterExecute(this);
		}

		private SqlStatement FindInTree(SqlStatement root, string label) {
			var statement = root;
			while (statement != null) {
				if (statement is CodeBlockStatement) {
					var block = (CodeBlockStatement) statement;
					if (String.Equals(label, block.Label))
						return statement;

					foreach (var child in block.Statements) {
						var found = FindInTree(child, label);
						if (found != null)
							return found;
					}
				}

				statement = statement.Parent;
			}

			return null;
		}

		public ExecutionContext NewBlock(SqlStatement statement) {
			return new ExecutionContext(this, Request.CreateBlock(), statement);
		}
	}
}
