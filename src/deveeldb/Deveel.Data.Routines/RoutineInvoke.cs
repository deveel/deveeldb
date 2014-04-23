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
using System.Text;

using Deveel.Data.DbSystem;

namespace Deveel.Data.Routines {
	/// <summary>
	/// A definition of a function including its name and parameters.
	/// </summary>
	/// <remarks>
	/// A <see cref="RoutineInvoke"/> can easily be transformed into a 
	/// <see cref="IFunction"/> object via a set of <see cref="LegacyFunctionFactory"/> instances.
	/// <para>
	/// <b>Note</b>: This object is <b>not</b> immutable or thread-safe. 
	/// A <see cref="RoutineInvoke"/> should not  be shared among different threads.
	/// </para>
	/// </remarks>
	[Serializable]
	public sealed class RoutineInvoke : ICloneable {
		/// <summary>
		/// A cached <see cref="IFunction"/> object that was generated when this 
		/// <see cref="RoutineInvoke"/> was looked up. The <see cref="IFunction"/> 
		/// object is transient.
		/// </summary>
		private IRoutine cachedFunction;


		public RoutineInvoke(String name, Expression[] parameterss) {
			Name = name;
			Arguments = parameterss;
		}

		/// <summary>
		/// Gets the name of the function.
		/// </summary>
		public string Name { get; internal set; }

		/// <summary>
		/// The list of parameters that are passed to the function.
		/// </summary>
		public Expression[] Arguments { get; private set; }

		public bool IsGlobArguments {
			get { return Arguments.Length == 1 && Arguments[0].Text.ToString().Equals("*"); }
		}

		/// <summary>
		/// Returns true if this function is an aggregate, or the parameters 
		/// are aggregates.
		/// </summary>
		/// <param name="context"></param>
		/// <remarks>
		/// It requires a <see cref="IQueryContext"/> object to lookup the 
		/// function in the function factory database.
		/// </remarks>
		/// <returns></returns>
		public bool IsAggregate(IQueryContext context) {
			IRoutineResolver resolver = context.RoutineResolver;
			bool isAggregate = resolver.IsAggregateFunction(this, context);
			if (isAggregate) {
				return true;
			}
			// Look at parameterss
			Expression[] parameters = Arguments;
			for (int i = 0; i < parameters.Length; ++i) {
				isAggregate = parameters[i].HasAggregateFunction(context);
				if (isAggregate) {
					return true;
				}
			}
			// No
			return false;
		}

		/// <summary>
		/// Returns a <see cref="IFunction"/> object from this <see cref="RoutineInvoke"/>.
		/// </summary>
		/// <param name="context"></param>
		/// <remarks>
		/// Note that two calls to this method will produce the same <see cref="IFunction"/> 
		/// object, however the same <see cref="IFunction"/> object will not be produced 
		/// over multiple instances of <see cref="RoutineInvoke"/> even when they represent 
		/// the same thing.
		/// </remarks>
		/// <returns></returns>
		public IRoutine GetFunction(IQueryContext context) {
			if (cachedFunction != null)
				return cachedFunction;

			IRoutineResolver lookup;
			if (context == null) {
				lookup = SystemFunctions.Factory;
			} else {
				lookup = context.RoutineResolver;
			}
			
			cachedFunction = lookup.ResolveRoutine(this, context);
			if (cachedFunction == null)
				throw new StatementException("IFunction '" + Name + "' doesn't exist.");

			return cachedFunction;
		}

		/// <inheritdoc/>
		public object Clone() {
			RoutineInvoke v = (RoutineInvoke)MemberwiseClone();
			// Deep clone the parameters
			Expression[] exps = (Expression[])v.Arguments.Clone();
			// Clone each element of the array
			for (int n = 0; n < exps.Length; ++n)
				exps[n] = (Expression)exps[n].Clone();
			v.Arguments = exps;
			v.cachedFunction = null;
			return v;
		}

		/// <inheritdoc/>
		public override String ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append(Name);
			buf.Append('(');
			for (int i = 0; i < Arguments.Length; ++i) {
				buf.Append(Arguments[i].Text.ToString());
				if (i < Arguments.Length - 1) {
					buf.Append(',');
				}
			}
			buf.Append(')');
			return buf.ToString();
		}

	}
}