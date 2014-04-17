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

using Deveel.Data.DbSystem;

namespace Deveel.Data.Routines {
	/// <summary>
	/// Provides convenience methods for handling aggregate functions (functions
	/// that are evaluated over a grouping set).
	/// </summary>
	/// <remarks>
	/// Note that this class handles the most common form of aggregate functions.
	/// These are aggregates with no more or no less than one parameter, and that 
	/// return NULL if the group set has a length of 0.  If an aggregate function 
	/// doesn't fit this design, then the developer must roll their own 
	/// <see cref="Function"/> to handle it.
	/// <para>
	/// This object handles full expressions being passed as parameters to the
	/// aggregate function.  The expression is evaluated for each set in the group.
	/// Therefore the aggregate function, avg(length(description)) will find the 
	/// average length of the description column.  sum(price * quantity) will find the 
	/// sum of the price * quantity of each set in the group.
	/// </para>
	/// </remarks>
	public abstract class AggregateFunction : Function {
		protected AggregateFunction(String name, Expression[] parameters)
			: base(name, parameters) {

			// Aggregates must have only one argument
			if (ParameterCount != 1)
				throw new Exception("'" + name + "' function must have one argument.");
		}

		// ---------- Abstract ----------

		/// <summary>
		/// Evaluates the aggregate function for the given values and 
		/// returns the result. 
		/// </summary>
		/// <param name="group"></param>
		/// <param name="context"></param>
		/// <param name="val1"></param>
		/// <param name="val2"></param>
		/// <remarks>
		/// If this aggregate was 'sum' then this method would sum the two 
		/// values. If this aggregate was 'avg' then this method would also 
		/// sum the two values and the <see cref="PostEvalAggregate"/> would 
		/// divide by the number processed.
		/// <para>
		/// <b>Note</b>: This first time this method is called on a set, 
		/// <paramref name="val1"/> is <b>null</b> and <paramref name="val2"/> 
		/// contains the first value in the set.
		/// </para>
		/// </remarks>
		/// <returns></returns>
		protected abstract TObject EvalAggregate(IGroupResolver group, IQueryContext context, TObject val1, TObject val2);

		/// <summary>
		/// Called just before the value is returned to the parent.
		/// </summary>
		/// <param name="group"></param>
		/// <param name="context"></param>
		/// <param name="result"></param>
		/// <returns></returns>
		/// <remarks>
		/// This does any final processing on the result before it is returned. 
		/// If this aggregate was 'avg' then we'd divide by the size of the group.
		/// </remarks>
		protected virtual TObject PostEvalAggregate(IGroupResolver group, IQueryContext context, TObject result) {
			// By default, do nothing....
			return result;
		}

		public override TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context) {
			if (group == null) {
				throw new Exception("'" + Name + "' can only be used as an aggregate function.");
			}

			TObject result = null;
			// All aggregates functions return 'null' if group size is 0
			int size = group.Count;
			if (size == 0) {
				// Return a NULL of the return type
				return new TObject(ReturnTType(resolver, context), null);
			}

			TObject val;
			VariableName v = this[0].AsVariableName();
			// If the aggregate parameter is a simple variable, then use optimal
			// routine,
			if (v != null) {
				for (int i = 0; i < size; ++i) {
					val = group.Resolve(v, i);
					result = EvalAggregate(group, context, result, val);
				}
			} else {
				// Otherwise we must resolve the expression for each entry in group,
				// This allows for expressions such as 'sum(quantity * price)' to
				// work for a group.
				Expression exp = this[0];
				for (int i = 0; i < size; ++i) {
					val = exp.Evaluate(null, group.GetVariableResolver(i), context);
					result = EvalAggregate(group, context, result, val);
				}
			}

			// Post method.
			result = PostEvalAggregate(group, context, result);

			return result;
		}
	}
}