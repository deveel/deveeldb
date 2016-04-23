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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Routines {
	/// <summary>
	/// Encapsulates the 
	/// </summary>
	public sealed class InvokeContext : IDisposable {
		private Field[] evaluatedArgs;
		private Dictionary<string, Field> output; 

		internal InvokeContext(Invoke invoke, IRoutine routine, IVariableResolver resolver, IGroupResolver group, IRequest request) {
			if (invoke == null)
				throw new ArgumentNullException("invoke");
			if (routine == null)
				throw new ArgumentNullException("routine");

			Request = request;
			GroupResolver = group;
			VariableResolver = resolver;
			Invoke = invoke;
			Routine = routine;
		}

		~InvokeContext() {
			Dispose(false);
		}

		/// <summary>
		/// Gets the information about the invoke of the routine.
		/// </summary>
		/// <seealso cref="Routines.Invoke"/>
		public Invoke Invoke { get; private set; }

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
		/// <seealso cref="Routines.Invoke.Arguments"/>
		/// <seealso cref="Invoke"/>
		public InvokeArgument[] Arguments {
			get { return Invoke.Arguments; }
		}

		public IVariableResolver VariableResolver { get; private set; }

		public IGroupResolver GroupResolver { get; private set; }

		public IRequest Request { get; private set; }

		public InvokeContext New(SqlExpression[] args) {
			var newInvoke = new Invoke(Invoke.RoutineName, args);
			return new InvokeContext(newInvoke, Routine, VariableResolver, GroupResolver, Request);
		}

		public Field[] EvaluatedArguments {
			get {
				if (evaluatedArgs == null) {
					// TODO: Sort them in correspondence with the routine parameter order
					evaluatedArgs = new Field[Arguments.Length];
					for (int i = 0; i < Arguments.Length; i++) {
						evaluatedArgs[i] = Arguments[i].Value.EvaluateToConstant(Request, VariableResolver);
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

		public InvokeResult Result(Field value) {
			if (Routine.Type != RoutineType.Function)
				throw new InvalidOperationException("The routine is not a function.");

			return new InvokeResult(this, value);
		}

		public InvokeResult Result() {
			if (Routine.Type != RoutineType.Procedure)
				throw new InvalidOperationException("The routine is not a procedure: a return value is required.");

			return new InvokeResult(this);
		}

		private RoutineParameter GetParameter(string name) {
			return Routine.RoutineInfo.Parameters.FirstOrDefault(x => x.Name.Equals(name, StringComparison.Ordinal));
		}

		public void SetOutput(string argName, Field value) {
			var parameter = GetParameter(argName);
			if (parameter == null)
				throw new InvalidOperationException(String.Format("Routine {0} has none parameter named '{1}'.", Routine.RoutineInfo.RoutineName, argName));
			if (!parameter.IsOutput)
				throw new InvalidOperationException();

			if (!parameter.IsNullable &&
				value.IsNull)
				throw new ArgumentException();

			if (!parameter.IsUnbounded)
				throw new ArgumentException();

			if (!parameter.Type.IsComparable(value.Type))
				throw new ArgumentException();

			if (output == null)
				output = new Dictionary<string, Field>();

			output[argName] = value;
		}

		private void Dispose(bool disposing) {
			Routine = null;
			Invoke = null;
			evaluatedArgs = null;
			output = null;
		}

		public void Dispose() {
			Dispose(true);
			GC.SuppressFinalize(this);
		}
	}
}