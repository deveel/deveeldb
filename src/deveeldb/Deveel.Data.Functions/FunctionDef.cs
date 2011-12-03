// 
//  Copyright 2010  Deveel
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

namespace Deveel.Data.Functions {
	/// <summary>
	/// A definition of a function including its name and parameters.
	/// </summary>
	/// <remarks>
	/// A <see cref="FunctionDef"/> can easily be transformed into a 
	/// <see cref="IFunction"/> object via a set of <see cref="FunctionFactory"/> instances.
	/// <para>
	/// <b>Note</b>: This object is <b>not</b> immutable or thread-safe. 
	/// A <see cref="FunctionDef"/> should not  be shared among different threads.
	/// </para>
	/// </remarks>
	[Serializable]
	public sealed class FunctionDef : ICloneable {
		/// <summary>
		/// The name of the function.
		/// </summary>
		private readonly String name;

		/// <summary>
		/// The list of parameters for the function.
		/// </summary>
		private Expression[] parameterss;

		/// <summary>
		/// A cached <see cref="IFunction"/> object that was generated when this 
		/// <see cref="FunctionDef"/> was looked up. The <see cref="IFunction"/> 
		/// object is transient.
		/// </summary>
		private IFunction cached_function;


		public FunctionDef(String name, Expression[] parameterss) {
			this.name = name;
			this.parameterss = parameterss;
		}

		/// <summary>
		/// Gets the name of the function.
		/// </summary>
		public string Name {
			get { return name; }
		}

		/// <summary>
		/// The list of parameters that are passed to the function.
		/// </summary>
		public Expression[] Parameters {
			get { return parameterss; }
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
			IFunctionLookup fun_lookup = context.FunctionLookup;
			bool is_aggregate = fun_lookup.IsAggregate(this);
			if (is_aggregate) {
				return true;
			}
			// Look at parameterss
			Expression[] parameters = Parameters;
			for (int i = 0; i < parameters.Length; ++i) {
				is_aggregate = parameters[i].HasAggregateFunction(context);
				if (is_aggregate) {
					return true;
				}
			}
			// No
			return false;
		}

		/// <summary>
		/// Returns a <see cref="IFunction"/> object from this <see cref="FunctionDef"/>.
		/// </summary>
		/// <param name="context"></param>
		/// <remarks>
		/// Note that two calls to this method will produce the same <see cref="IFunction"/> 
		/// object, however the same <see cref="IFunction"/> object will not be produced 
		/// over multiple instances of <see cref="FunctionDef"/> even when they represent 
		/// the same thing.
		/// </remarks>
		/// <returns></returns>
		public IFunction GetFunction(IQueryContext context) {
			if (cached_function != null)
				return cached_function;
			IFunctionLookup lookup;
			if (context == null) {
				lookup = FunctionFactory.Default;
			} else {
				lookup = context.FunctionLookup;
			}
			
			cached_function = lookup.GenerateFunction(this);
			if (cached_function == null)
				throw new StatementException("IFunction '" + Name + "' doesn't exist.");
			return cached_function;
		}

		/// <inheritdoc/>
		public object Clone() {
			FunctionDef v = (FunctionDef)MemberwiseClone();
			// Deep clone the parameters
			Expression[] exps = (Expression[])v.parameterss.Clone();
			// Clone each element of the array
			for (int n = 0; n < exps.Length; ++n)
				exps[n] = (Expression)exps[n].Clone();
			v.parameterss = exps;
			v.cached_function = null;
			return v;
		}

		/// <inheritdoc/>
		public override String ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append(name);
			buf.Append('(');
			for (int i = 0; i < parameterss.Length; ++i) {
				buf.Append(parameterss[i].Text.ToString());
				if (i < parameterss.Length - 1) {
					buf.Append(',');
				}
			}
			buf.Append(')');
			return buf.ToString();
		}

	}
}