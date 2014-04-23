// 
//  Copyright 2010-2014 Deveel
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

using System;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Routines {
	public sealed class ExecuteContext {
		private TObject[] evaluatedArgs;

		internal ExecuteContext(RoutineInvoke invoke, IRoutine routine, IVariableResolver resolver, IGroupResolver group, IQueryContext queryContext) {
			if (invoke == null)
				throw new ArgumentNullException("invoke");
			if (routine == null)
				throw new ArgumentNullException("routine");

			QueryContext = queryContext;
			GroupResolver = group;
			VariableResolver = resolver;
			Invoke = invoke;
			Routine = routine;
		}

		public RoutineInvoke Invoke { get; private set; }

		public IRoutine Routine { get; set; }

		public RoutineType RoutineType {
			get { return Routine.Type; }
		}

		public Expression[] Arguments {
			get { return Invoke.Arguments; }
		}

		public IVariableResolver VariableResolver { get; private set; }

		public IGroupResolver GroupResolver { get; private set; }

		public IQueryContext QueryContext { get; private set; }

		public TObject[] EvaluatedArguments {
			get {
				if (evaluatedArgs == null) {
					evaluatedArgs = new TObject[Arguments.Length];
					for (int i = 0; i < Arguments.Length; i++) {
						evaluatedArgs[i] = Arguments[i].Evaluate(GroupResolver, VariableResolver, QueryContext);
					}
				}

				return evaluatedArgs;
			}
		}

		public int ArgumentCount {
			get { return Arguments == null ? 0 : Arguments.Length; }
		}

		public ExecuteResult FunctionResult(TObject returnValue) {
			return new ExecuteResult(this) {
				ReturnValue = returnValue
			};
		}

		public ExecuteResult Result() {
			return new ExecuteResult(this);
		}
	}
}