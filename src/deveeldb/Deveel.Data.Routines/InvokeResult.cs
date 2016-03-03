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
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Sql;

namespace Deveel.Data.Routines {
	/// <summary>
	/// Represents the result of the execution of a routine.
	/// </summary>
	public sealed class InvokeResult {
		private Dictionary<string, Field> outputValues;

		private InvokeResult(InvokeContext context, Field returnValue, bool hasReturn) {
			Context = context;
			ReturnValue = returnValue;
			HasReturnValue = hasReturn;
		}

		internal InvokeResult(InvokeContext context) 
			: this(context, Field.Null(), false) {
		}

		internal InvokeResult(InvokeContext context, Field returnValue)
			: this(context, returnValue, true) {
		}

		/// <summary>
		/// Gets the parent context that originated the result.
		/// </summary>
		public InvokeContext Context { get; private set; }

		/// <summary>
		/// If the context of the result is a function, gets the return value of
		/// the function.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This is set to <see cref="Field.Null()"/> by default: the property
		/// <see cref="HasReturnValue"/> assess a return value was really provided 
		/// by the routine (if the routine is a function).
		/// </para>
		/// </remarks>
		/// <seealso cref="HasReturnValue"/>
		public Field ReturnValue { get; private set; }

		/// <summary>
		/// Gets a boolean value indicating if the function has a <see cref="ReturnValue"/>.
		/// </summary>
		/// <remarks>
		/// This is always set to <c>false</c> when the routine context is of a
		/// <c>PROCEDURE</c>, that has no return value by definition.
		/// </remarks>
		/// <seealso cref="ReturnValue"/>
		public bool HasReturnValue { get; private set; }

		/// <summary>
		/// Gets a boolean value indicating if the routine has any <c>OUT</c>
		/// parameter set.
		/// </summary>
		/// <seealso cref="OutputParameters"/>
		public bool HasOutputParameters {
			get { return outputValues != null && outputValues.Count > 0; }
		}

		/// <summary>
		/// Gets a dictionary of the <c>OUT</c> parameters emitted byt the routine.
		/// </summary>
		public IDictionary<string, Field> OutputParameters {
			get {
				if (outputValues == null)
					return new Dictionary<string, Field>();

				return outputValues.ToDictionary(x => x.Key, y => y.Value);
			}
		}  

		internal void SetOutputParameter(string name, Field value) {
			if (Context.RoutineType != RoutineType.Procedure)
				throw new Exception("Cannot set an output parameter value for a function.");

			if (outputValues == null)
				outputValues = new Dictionary<string, Field>();

			outputValues[name] = value;
		}
	}
}