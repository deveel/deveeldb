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
using System.Threading.Tasks;

using Deveel.Data.Events;
using Deveel.Data.Services;

namespace Deveel.Data.Sql.Statements {
	public class StatementContext : Context {
		public StatementContext(IContext parent, SqlStatement statement) 
			: base(parent, KnownScopes.Statement) {
			Statement = statement ?? throw new ArgumentNullException(nameof(statement));
			Result = new EmptyStatementResult();
		}

		public SqlStatement Statement { get; }

		public IStatementResult Result { get; private set; }

		public bool HasResult => !(Result is EmptyStatementResult);

		internal bool WasTerminated { get; set; }

		private void Terminate() {
			WasTerminated = true;

			if (Parent != null &&
				Parent is StatementContext) {
				var context = (StatementContext) Parent;
				context.Result = Result;
				context.Terminate();
			}
		}

		private void ThrowIfTerminated() {
			if (WasTerminated)
				throw new InvalidOperationException("The statement context was terminated");
		}


		public void SetResult(IStatementResult result) {
			ThrowIfTerminated();

			Result = result;
		}

		public void SetResult(SqlObject value)
			=> SetResult(new StatementExpressionResult(value));

		public void Return(IStatementResult result) {
			SetResult(result);
			Terminate();
		}

		public void Return(SqlObject value)
			=> Return(new StatementExpressionResult(value));

		public async Task TransferAsync(string label) {
			ThrowIfTerminated();

			if (String.IsNullOrEmpty(label))
				throw new ArgumentNullException(nameof(label));

			var statement = FindInTree(Statement, label);
			if (statement == null)
				throw new SqlStatementException($"Could not find any block labeled '{label}' in the execution tree.");

			using (var block = NewBlock(statement)) {
				await statement.ExecuteAsync(block);

				if (block.HasResult && !WasTerminated)
					SetResult(block.Result);
			}
		}

		private StatementContext NewBlock(SqlStatement statement) {
			return new StatementContext(this, statement);
		}

		private SqlStatement FindInTree(SqlStatement reference, string label) {
			var found = FindInTree(reference, label, true);
			if (found != null)
				return found;

			return FindInTree(reference, label, false);
		}

		private SqlStatement FindInTree(SqlStatement reference, string label, bool forward) {
			var statement = reference;
			while (statement != null) {
				if (statement is ILabeledStatement) {
					var block = (ILabeledStatement) statement;
					if (String.Equals(label, block.Label, StringComparison.Ordinal))
						return statement;
				}

				if (statement is IStatementContainer) {
					var container = (IStatementContainer)statement;
					foreach (var child in container.Statements) {
						var found = FindInTree(child, label, true);
						if (found != null)
							return found;
					}
				}

				statement = forward ? statement.Next : statement.Previous;
			}

			if (reference.Parent != null && !forward)
				return FindInTree(reference.Parent, label, false);

			return null;
		}

		#region Loop
		
		//public void ControlLoop(LoopControlType controlType, string label) {
		//	ThrowIfTerminated();

		//	var loop = FindLoopInTree(Statement, label);

		//	if (loop == null)
		//		throw new SqlStatementException("Could not find the loop");

		//	loop.Control(controlType);
		//}

		//private LoopStatement FindLoopInTree(SqlStatement reference, string label) {
		//	var found = FindLoopInTree(reference, label, true);
		//	if (found != null)
		//		return found;

		//	return FindLoopInTree(reference, label, false);
		//}

		//private LoopStatement FindLoopInTree(SqlStatement reference, string label, bool forward) {
		//	if (!String.IsNullOrWhiteSpace(label)) {
		//		return FindInTree(reference, label, false) as LoopStatement;
		//	}

		//	var statement = reference;
		//	while (statement != null) {
		//		if (statement is LoopStatement) {
		//			return statement as LoopStatement;
		//		}

		//		if (statement is IStatementContainer) {
		//			var container = (IStatementContainer) statement;
		//			foreach (var child in container.Statements) {
		//				var loop = FindLoopInTree(child, null, true);
		//				if (loop != null)
		//					return loop;
		//			}
		//		}

		//		statement = forward ? statement.Next : statement.Previous;
		//	}

		//	if (reference.Parent != null && !forward)
		//		return FindLoopInTree(reference.Parent, label, false);

		//	return null;
		//}

		#endregion
	}
}