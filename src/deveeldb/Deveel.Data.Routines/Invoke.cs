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
using System.Linq;
using System.Text;

using Deveel.Data;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Routines {
	/// <summary>
	/// The information about the invocation of a routine, including
	/// the full name and arguments (as <see cref="SqlExpression"/>).
	/// </summary>
	public sealed class Invoke {
		private IRoutine cached;

		/// <summary>
		/// Constructs a new <see cref="Invoke"/> with the given
		/// name of the routine and no arguments.
		/// </summary>
		/// <param name="routineName">The fully qualified name of the routine
		/// to be invoked.</param>
		public Invoke(ObjectName routineName)
			: this(routineName, new SqlExpression[0]) {
		}

		/// <summary>
		/// Constructs a new <see cref="Invoke"/> with the given
		/// name of the routine and the arguments.
		/// </summary>
		/// <param name="routineName">The fully qualified name of the routine
		/// to be invoked.</param>
		/// <param name="arguments">The arguments to pass to the routine.</param>
		public Invoke(ObjectName routineName, SqlExpression[] arguments) {
			RoutineName = routineName;
			Arguments = arguments;
		}

		/// <summary>
		/// Gets the fully qualified name of the routine to invoke.
		/// </summary>
		public ObjectName RoutineName { get; private set; }

		/// <summary>
		/// Gets an array of arguments to be passed to the invoked routine.
		/// </summary>
		public SqlExpression[] Arguments { get; private set; }

		/// <summary>
		/// Gets a boolean value indicating if the arguments of the invocation represent
		/// a single <c>glob</c> (*).
		/// </summary>
		/// <remarks>
		/// The <c>glob</c> argument is a special one and it is used for functions
		/// like <c>COUNT</c>.
		/// </remarks>
		public bool IsGlobArgument {
			get {
				return Arguments != null &&
				       Arguments.Length == 1 &&
				       Arguments[0] is SqlConstantExpression &&
				       ((SqlConstantExpression) Arguments[0]).Value.Value.ToString() == "*";
			}
		}

		/// <summary>
		/// Checks if the target of the invocation is an aggregate function.
		/// </summary>
		/// <param name="context">The query context used to resolve the routine.</param>
		/// <returns>
		/// Returns <c>true</c> if the target routine of the invocation is a <see cref="IFunction"/>
		/// and the <see cref="IFunction.FunctionType"/> is <see cref="FunctionType.Aggregate"/>,
		/// otherwise it returns <c>false</c>.
		/// </returns>
		public bool IsAggregate(IQueryContext context) {
			if (context.IsAggregateFunction(this))
				return true;

			// Look at parameterss
			return Arguments.Any(x => x.HasAggregate(context));
		}

		/// <summary>
		/// Resolves the routine target of the invocation within the give context.
		/// </summary>
		/// <param name="context">The query context used to resolve the routine.</param>
		/// <remarks>
		/// <para>
		/// If the given <paramref name="context"/> is <c>null</c> this method will
		/// try to resolve the routine towards the 
		/// </para>
		/// </remarks>
		/// <returns>
		/// Returns an instance of <see cref="IRoutine"/> that is the target of the invocation.
		/// </returns>
		/// <exception cref="InvalidOperationException">
		/// If the routine could not be resolved for this call.
		/// </exception>
		public IRoutine ResolveRoutine(IQueryContext context) {
			if (cached != null)
				return cached;

			if (context == null) {
				cached = SystemFunctions.Provider.ResolveFunction(this, null);
			} else {
				cached = context.ResolveRoutine(this);
			}

			if (cached == null)
				throw new InvalidOperationException(String.Format("Unable to resolve the call {0} to a function", this));

			return cached;
		}

		public IFunction ResolveFunction(IQueryContext context) {
			return ResolveRoutine(context) as IFunction;
		}

		public IProcedure ResolveProcedure(IQueryContext context) {
			return ResolveRoutine(context) as IProcedure;
		}
		
		/// <summary>
		/// Resolves this routine invocation to a system function.
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="IFunction"/> that is defined in
		/// the <see cref="SystemFunctions"/> domain.
		/// </returns>
		/// <seealso cref="ResolveRoutine"/>
		public IFunction ResolveSystemFunction() {
			return ResolveRoutine(null) as IFunction;
		}

		public ExecuteResult Execute() {
			return Execute(null);
		}

		public ExecuteResult Execute(IQueryContext context) {
			return Execute(context, null);
		}

		public ExecuteResult Execute(IQueryContext context, IVariableResolver resolver) {
			return Execute(context, resolver, null);
		}

		public ExecuteResult Execute(IQueryContext context, IVariableResolver resolver, IGroupResolver group) {
			var routine = ResolveRoutine(context);
			var executeContext = new ExecuteContext(this, routine, resolver, group, context);
			return routine.Execute(executeContext);
		}

		public override String ToString() {
			var buf = new StringBuilder();
			buf.Append(RoutineName);
			buf.Append('(');
			for (int i = 0; i < Arguments.Length; ++i) {
				buf.Append(Arguments[i]);
				if (i < Arguments.Length - 1) {
					buf.Append(',');
				}
			}
			buf.Append(')');
			return buf.ToString();
		}
	}
}
