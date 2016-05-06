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
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Routines {
	/// <summary>
	/// Represents the result of the execution of a routine.
	/// </summary>
	public sealed class InvokeResult {
		private InvokeResult(InvokeContext context, object returnValue, bool hasReturn) {
			Context = context;
			if (returnValue is ITable) {
				var table = (ITable) returnValue;
				ReturnTable = table;
				HasReturnTable = hasReturn;
			} else if (returnValue is Field) {
				var field = (Field) returnValue;
				ReturnValue = field;
				HasReturnValue = hasReturn;
			}
		}

		internal InvokeResult(InvokeContext context) 
			: this(context, Field.Null(), false) {
		}

		internal InvokeResult(InvokeContext context, Field returnValue)
			: this(context, returnValue, true) {
		}

		internal InvokeResult(InvokeContext context, ITable table)
			: this(context, table, true) {
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
		/// Gets a boolean value indicating if the invoke has a <see cref="ReturnValue"/>.
		/// </summary>
		/// <remarks>
		/// This is always set to <c>false</c> when the routine context is of a
		/// <c>PROCEDURE</c>, that has no return value by definition.
		/// </remarks>
		/// <seealso cref="ReturnValue"/>
		public bool HasReturnValue { get; private set; }

		/// <summary>
		/// If the context of the result is a function, gets the return table of
		/// the function.
		/// </summary>
		/// <remarks>
		/// <para>
		/// This is set to <c>null</c> by default: the property
		/// <see cref="HasReturnTable"/> assess a return table was really provided 
		/// by the function.
		/// </para>
		/// </remarks>
		/// <seealso cref="HasReturnTable"/>
		public ITable ReturnTable { get; private set; }

		/// <summary>
		/// Gets a boolean value indicating if the function has a <see cref="ReturnTable"/>.
		/// </summary>
		public bool HasReturnTable { get; private set; }

		/// <summary>
		/// Gets a boolean value indicating if the routine has any <c>OUT</c>
		/// parameter set.
		/// </summary>
		/// <seealso cref="OutputParameters"/>
		public bool HasOutputParameters {
			get { return Context.Output != null && Context.Output.Count > 0; }
		}

		/// <summary>
		/// Gets a dictionary of the <c>OUT</c> parameters emitted by the routine.
		/// </summary>
		public IDictionary<string, Field> OutputParameters {
			get { return Context.Output; }
		}  
	}
}