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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	/// <summary>
	/// An abstract implementation of <see cref="IFunction"/>.
	/// </summary>
	public abstract class InvokedFunction : IFunction {
		public static readonly TType DynamicType = new TDynamicType();

		/// <summary>
		/// The list of expressions this function has as parameters.
		/// </summary>
		private readonly Expression[] parameters;


		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <param name="parameters"></param>
		protected InvokedFunction(string name, Expression[] parameters) {
			Name = name;
			this.parameters = parameters;
		}


		/// <summary>
		/// Returns the number of parameters for this function.
		/// </summary>
		public int ParameterCount {
			get { return parameters.Length; }
		}


		/// <summary>
		/// Returns the parameter at the given index in the parameters list.
		/// </summary>
		/// <param name="n"></param>
		/// <returns></returns>
		public Expression this[int n] {
			get { return parameters[n]; }
		}

		/// <summary>
		/// Returns true if the param is the special case glob parameter (*).
		/// </summary>
		public bool IsGlob {
			get {
				if (parameters == LegacyFunctionFactory.GlobList) {
					return true;
				}
				if (parameters.Length == 1) {
					Expression exp = parameters[0];
					return (exp.Count == 1 && exp.Text.ToString().Equals("*"));
				}
				return false;
			}
		}


		// ---------- Implemented from IFunction ----------

		/// <summary>
		/// Returns the name of the function.
		/// </summary>
		/// <remarks>
		/// The name is a unique identifier that can be used to recreate 
		/// this function.  This identifier can be used to easily serialize 
		/// the function when grouped with its parameters.
		/// </remarks>
		public string Name { get; private set; }


		/// <summary>
		/// Evaluates the function and returns a TObject that represents the result of the function.
		/// </summary>
		/// <param name="group"></param>
		/// <param name="resolver"></param>
		/// <param name="context"></param>
		/// <remarks>
		/// The <see cref="IVariableResolver"/> object should be used to look up 
		/// variables in the parameter of the function. The  <see cref="FunctionTable"/> 
		/// object should only be used when the function is a grouping function. 
		/// For example, <c>avg(value_of)</c>.
		/// </remarks>
		/// <returns></returns>
		public abstract TObject Evaluate(IGroupResolver group, IVariableResolver resolver, IQueryContext context);


		/// <summary>
		/// The type of object this function returns. 
		/// </summary>
		/// <param name="resolver"></param>
		/// <param name="context"></param>
		/// <remarks>
		/// By default this method returns a <see cref="PrimitiveTypes.Numeric"/>.
		/// </remarks>
		/// <returns></returns>
		/// <seealso cref="ReturnTType()"/>
		public virtual TType ReturnTType(IVariableResolver resolver, IQueryContext context) {
			return ReturnTType();
		}

		/// <summary>
		/// The type of object this function returns. 
		/// </summary>
		/// <remarks>
		/// By default this method returns a <see cref="PrimitiveTypes.Numeric"/>.
		/// </remarks>
		/// <returns></returns>
		/// <seealso cref="ReturnTType(IVariableResolver,IQueryContext)"/>
		protected virtual TType ReturnTType() {
			return PrimitiveTypes.Numeric;
		}

		/// <inheritdoc/>
		public override String ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append(Name);
			buf.Append('(');
			for (int i = 0; i < parameters.Length; ++i) {
				buf.Append(parameters[i].Text.ToString());
				if (i < parameters.Length - 1) {
					buf.Append(',');
				}
			}
			buf.Append(')');
			return buf.ToString();
		}

		#region TDynamicType

		class TDynamicType : TType {
			public TDynamicType() 
				: base(-5000) {
			}

			public override DbType DbType {
				get { return DbType.Object; }
			}

			public override int Compare(object x, object y) {
				throw new NotSupportedException();
			}

			public override bool IsComparableType(TType type) {
				return true;
			}

			public override int CalculateApproximateMemoryUse(object ob) {
				return 0;
			}
		}

		#endregion
	}
}