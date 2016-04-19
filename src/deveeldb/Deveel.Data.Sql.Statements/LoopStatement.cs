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
using System.Runtime.Serialization;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public class LoopStatement : CodeBlockStatement, IPlSqlStatement {
		public LoopStatement() {
		}

		protected LoopStatement(SerializationInfo info, StreamingContext context)
			: base(info, context) {
		}

		private bool Continue { get; set; }

		private bool Exit { get; set; }

		internal void Control(LoopControlType controlType) {
			if (controlType == LoopControlType.Continue) {
				Continue = true;
			} else if (controlType == LoopControlType.Exit) {
				Exit = true;
			}
		}

		// TODO: Review the logic to control the loop...
		protected virtual bool Loop(ExecutionContext context) {
			return !Exit && !context.HasTermination;
		}

		protected virtual bool CanExecute(ExecutionContext context) {
			return !Continue && !context.HasTermination;
		}

		protected virtual void BeforeLoop(ExecutionContext context) {
		}

		protected virtual void AfterLoop(ExecutionContext context) {
		}

		protected virtual LoopStatement CreateNew() {
			return new LoopStatement { Label = Label };
		}

		protected override SqlStatement PrepareStatement(IRequest context) {
			if (!LoopBreakChecker.HasBreak(this))
				throw new InvalidOperationException("The loop has no possible exit");


			var loop = CreateNew();

			foreach (var statement in Statements) {
				var prepared = statement.Prepare(context);

				if (prepared == null)
					throw new InvalidOperationException("The preparation of a child statement was invalid.");

				loop.Statements.Add(prepared);
			}
			
			return loop;
		}

		protected override void ExecuteBlock(ExecutionContext context) {
			BeforeLoop(context);

			while (Loop(context)) {
				if (CanExecute(context))
					base.ExecuteBlock(context);
			}

			AfterLoop(context);
		}
	}
}
