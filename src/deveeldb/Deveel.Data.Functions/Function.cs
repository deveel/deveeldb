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
using System.Text;

namespace Deveel.Data.Functions {
	/// <summary>
	/// An abstract implementation of <see cref="IFunction"/>.
	/// </summary>
	public abstract class Function : IFunction {

		/// <summary>
		/// The name of the function.
		/// </summary>
		private readonly string name;

		/// <summary>
		/// The list of expressions this function has as parameters.
		/// </summary>
		private readonly Expression[] parameters;

		/// <summary>
		/// Set to true if this is an aggregate function (requires a group). 
		/// It is false by default.
		/// </summary>
		private bool is_aggregate;

		/// <summary>
		/// 
		/// </summary>
		/// <param name="name"></param>
		/// <param name="parameters"></param>
		protected Function(String name, Expression[] parameters) {
			this.name = name;
			this.parameters = parameters;

			is_aggregate = false;
		}

		/// <summary>
		/// A method called from the constructor to set the function aggregate.
		/// </summary>
		/// <param name="status"></param>
		protected void SetAggregate(bool status) {
			is_aggregate = status;
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
				if (parameters == FunctionFactory.GlobList) {
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
		public string Name {
			get { return name; }
		}

		/// <summary>
		/// Returns the list of Variable objects that this function uses as its parameters.
		/// </summary>
		/// <remarks>
		/// If this returns an empty list, then the function must only have constant 
		/// parameters.  This information can be used to optimize evaluation because 
		/// if all the parameters of a function are constant then we only need to evaluate 
		/// the function once.
		/// </remarks>
		public virtual IList AllVariables {
			get {
				ArrayList result_list = new ArrayList();
				for (int i = 0; i < parameters.Length; ++i) {
					IList l = parameters[i].AllVariables;
					result_list.AddRange(l);
				}
				return result_list;
			}
		}

		/// <summary>
		/// Returns the list of all element objects that this function uses as its parameters.
		/// </summary>
		/// <remarks>
		/// If this returns an empty list, then the function has no input elements at 
		/// all. (something like: <c>upper(user())</c>)
		/// </remarks>
		public virtual IList AllElements {
			get {
				ArrayList result_list = new ArrayList();
				for (int i = 0; i < parameters.Length; ++i) {
					IList l = parameters[i].AllElements;
					result_list.AddRange(l);
				}
				return result_list;
			}
		}

		/// <summary>
		/// Returns true if this function is an aggregate function.
		/// </summary>
		/// <param name="context"></param>
		/// <remarks>
		/// An aggregate function requires that the IGroupResolver is not null when 
		/// the evaluate method is called.
		/// </remarks>
		/// <returns></returns>
		public bool IsAggregate(IQueryContext context) {
			if (is_aggregate) {
				return true;
			} else {
				// Check if arguments are aggregates
				for (int i = 0; i < parameters.Length; ++i) {
					Expression exp = parameters[i];
					if (exp.HasAggregateFunction(context)) {
						return true;
					}
				}
			}
			return false;
		}

		/// <summary>
		/// Prepares the exressions that are the parameters of this function.
		/// </summary>
		/// <param name="preparer"></param>
		/// <remarks>
		/// This is intended to be used if we need to resolve aspects such as 
		/// <see cref="VariableName"/> references. For example, a variable reference 
		/// to <i>number</i> may become <i>APP.Table.NUMBER</i>.
		/// </remarks>
		public void PrepareParameters(IExpressionPreparer preparer) {
			for (int i = 0; i < parameters.Length; ++i) {
				parameters[i].Prepare(preparer);
			}
		}

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
		/// Initializes the function.
		/// </summary>
		/// <param name="resolver"></param>
		/// <remarks>
		/// By default, this does nothing however this should be overwritten if it is 
		/// needed to check the parameter arguments.
		/// </remarks>
		public virtual void Init(IVariableResolver resolver) {
		}


		/// <summary>
		/// The type of object this function returns. 
		/// </summary>
		/// <param name="resolver"></param>
		/// <param name="context"></param>
		/// <remarks>
		/// By default this method returns a <see cref="TType.NumericType"/>.
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
		/// By default this method returns a <see cref="TType.NumericType"/>.
		/// </remarks>
		/// <returns></returns>
		/// <seealso cref="ReturnTType(IVariableResolver,IQueryContext)"/>
		protected virtual TType ReturnTType() {
			return TType.NumericType;
		}

		/// <inheritdoc/>
		public override String ToString() {
			StringBuilder buf = new StringBuilder();
			buf.Append(name);
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
	}
}