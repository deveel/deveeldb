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

using Deveel.Data.Security;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql {
	public sealed class ExecutionContext {
		public ExecutionContext(IRequest request, SqlStatement statement)
			: this(null, request, statement) {
		}

		private ExecutionContext(ExecutionContext parent, IRequest request, SqlStatement statement) {
			if (request == null)
				throw new ArgumentNullException("request");

			Request = request;
			Statement = statement;
			Parent = parent;
		}

		public SqlStatement Statement { get; private set; }

		public IRequest Request { get; private set; }

		public ITable Result { get; private set; }

		public bool HasTermination { get; private set; }

		public bool HasResult {
			get { return Result != null; }
		}

		public User User {
			get { return Request.Query.User(); }
		}

		public IQuery Query {
			get { return Request.Query; }
		}

		public SystemAccess DirectAccess {
			get { return Request.Access; }
		}

		private ExecutionContext Parent { get; set; }


		private void AssertNotFinished() {
			if (HasTermination)
				throw new InvalidOperationException("The context has already terminated.");
		}

		public void SetResult(ITable result) {
			AssertNotFinished();
			Result = result;
		}

		public void SetResult(int value) {
			SetResult(FunctionTable.ResultTable(Request, value));
		}

		public void Raise(string exceptionName) {
			if (String.IsNullOrEmpty(exceptionName))
				throw new ArgumentNullException("exceptionName");

			AssertNotFinished();

			try {
				var statement = Statement;
				while (statement != null) {
					if (statement is PlSqlBlockStatement) {
						var block = (PlSqlBlockStatement) statement;
						block.FireHandler(this, exceptionName);
						return;
					}

					statement = statement.Parent;
				}
			} finally {
				HasTermination = true;
			}
		}

		public void Control(LoopControlType controlType, string label) {
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

		public void Transfer(string label) {
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

		public void Return(SqlExpression value) {
			AssertNotFinished();

			if (value != null)
				SetResult(FunctionTable.ResultTable(Request, value));

			HasTermination = true;
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
			return new ExecutionContext(this, new Block(Request), statement);
		}
	}
}
