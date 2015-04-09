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

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Routines {
	/// <summary>
	/// Encapsulates the 
	/// </summary>
	public sealed class ExecuteContext {
		private DataObject[] evaluatedArgs;

		internal ExecuteContext(InvokeRequest request, IRoutine routine, IVariableResolver resolver, IGroupResolver group, IQueryContext queryContext) {
			if (request == null)
				throw new ArgumentNullException("request");
			if (routine == null)
				throw new ArgumentNullException("routine");

			QueryContext = queryContext;
			GroupResolver = group;
			VariableResolver = resolver;
			Request = request;
			Routine = routine;
		}

		/// <summary>
		/// Gets the information about the request of the routine.
		/// </summary>
		/// <seealso cref="InvokeRequest"/>
		public InvokeRequest Request { get; private set; }

		/// <summary>
		/// Gets the instance of the <see cref="IRoutine"/> being executed.
		/// </summary>
		/// <seealso cref="IRoutine"/>
		public IRoutine Routine { get; private set; }

		/// <summary>
		/// Gets the type of the routine being executed.
		/// </summary>
		/// <seealso cref="RoutineType"/>
		/// <seealso cref="IRoutine.Type"/>
		public RoutineType RoutineType {
			get { return Routine.Type; }
		}

		/// <summary>
		/// Gets the array of expressions forming the arguments of the execution.
		/// </summary>
		/// <seealso cref="SqlExpression"/>
		/// <seealso cref="InvokeRequest.Arguments"/>
		/// <seealso cref="Request"/>
		public SqlExpression[] Arguments {
			get { return Request.Arguments; }
		}

		public IVariableResolver VariableResolver { get; private set; }

		public IGroupResolver GroupResolver { get; private set; }

		public IQueryContext QueryContext { get; private set; }

		public DataObject[] EvaluatedArguments {
			get {
				if (evaluatedArgs == null) {
					evaluatedArgs = new DataObject[Arguments.Length];
					for (int i = 0; i < Arguments.Length; i++) {
						var exp = Arguments[i].Evaluate(QueryContext, VariableResolver, GroupResolver);
						if (!(exp is SqlConstantExpression))
							throw new InvalidOperationException();

						evaluatedArgs[i] = ((SqlConstantExpression) exp).Value;
					}
				}

				return evaluatedArgs;
			}
		}

		/// <summary>
		/// Gets a count of the arguments passed to the routine.
		/// </summary>
		/// <seealso cref="Arguments"/>
		public int ArgumentCount {
			get { return Arguments == null ? 0 : Arguments.Length; }
		}

		/// <summary>
		/// Forms a result that gets the returned value of a function.
		/// </summary>
		/// <param name="returnValue">The value returned by a function evaluation.</param>
		/// <returns>
		/// Returns an instance of <see cref="ExecuteResult"/> that has a function
		/// value set.
		/// </returns>
		/// <exception cref="InvalidOperationException">
		/// If the routine is not a function.
		/// </exception>
		/// <seealso cref="Type"/>
		/// <seealso cref="ExecuteResult"/>
		public ExecuteResult FunctionResult(DataObject returnValue) {
			if (RoutineType != RoutineType.Function)
				throw new InvalidOperationException(String.Format("The routine '{0}' is not a FUNCTION.", Request.RoutineName));

			return new ExecuteResult(this, returnValue);
		}

		/// <summary>
		/// Forms a result for a routine.
		/// </summary>
		/// <returns></returns>
		public ExecuteResult Result() {
			return new ExecuteResult(this);
		}
	}
}