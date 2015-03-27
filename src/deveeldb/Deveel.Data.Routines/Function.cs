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

using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	/// <summary>
	/// A system routine that returns a value at the end of its execution. 
	/// </summary>
	/// <remarks>
	/// This class provides the base features for constructing functions.
	/// </remarks>
	public abstract class Function : Routine, IFunction {
		/// <summary>
		/// A special <see cref="DataType"/> that is used to mark an argument
		/// of a function as <c>dynamic</c>.
		/// </summary>
		/// <remarks>
		/// This <see cref="DataType"/> matches against any passed object.
		/// </remarks>
		public static readonly DataType DynamicType = new DynamicDataType();

		/// <summary>
		/// Constructs a new <see cref="Function"/> with the given signature.
		/// </summary>
		/// <param name="name">The name that uniquely identifies the function
		/// within the system. This is a complex object <see cref="ObjectName"/> 
		/// that includes definiton of the location where the function is defined.</param>
		/// <param name="parameters">The parameter signature of the function.</param>
		/// <remarks>
		/// <para>
		/// This constructor does not provide any default return type, that means
		/// this value must be dynamically resolved by <see cref="ReturnType(ExecuteContext)"/>
		/// </para>
		/// </remarks>
		protected Function(ObjectName name, RoutineParameter[] parameters) 
			: this(name, parameters, Routines.FunctionType.Static) {
		}

		/// <summary>
		/// Constructs a new <see cref="Function"/> with the given signature.
		/// </summary>
		/// <param name="name">The name that uniquely identifies the function
		/// within the system. This is a complex object <see cref="ObjectName"/> 
		/// that includes definiton of the location where the function is defined.</param>
		/// <param name="parameters">The parameter signature of the function.</param>
		/// <param name="functionType">The type of the function.</param>
		/// <remarks>
		/// <para>
		/// This constructor does not provide any default return type, that means
		/// this value must be dynamically resolved by <see cref="ReturnType(ExecuteContext)"/>
		/// </para>
		/// </remarks>
		protected Function(ObjectName name, RoutineParameter[] parameters, FunctionType functionType) 
			: this(name, parameters, null, functionType) {
		}

		/// <summary>
		/// Constructs a new <see cref="Function"/> with the given signature.
		/// </summary>
		/// <param name="name">The name that uniquely identifies the function
		/// within the system. This is a complex object <see cref="ObjectName"/> 
		/// that includes definiton of the location where the function is defined.</param>
		/// <param name="parameters">The parameter signature of the function.</param>
		/// <param name="returnType">The type of the value returned by the function.</param>
		protected Function(ObjectName name, RoutineParameter[] parameters, DataType returnType) 
			: this(name, parameters, returnType, FunctionType.Static) {
		}

		/// <summary>
		/// Constructs a new <see cref="Function"/> with the given signature.
		/// </summary>
		/// <param name="name">The name that uniquely identifies the function
		/// within the system. This is a complex object <see cref="ObjectName"/> 
		/// that includes definiton of the location where the function is defined.</param>
		/// <param name="parameters">The parameter signature of the function.</param>
		/// <param name="returnType">The type of the value returned by the function.</param>
		/// <param name="functionType">The type of the function.</param>
		protected Function(ObjectName name, RoutineParameter[] parameters, DataType returnType, FunctionType functionType) 
			: base(name, parameters, RoutineType.Function) {
			this.returnType = returnType;
			FunctionType = functionType;
		}

		private readonly DataType returnType;

		public virtual FunctionType FunctionType { get; private set; }

		/// <summary>
		/// Executes the function and provides a result.
		/// </summary>
		/// <param name="context">The context of the execution.</param>
		/// <returns>
		/// Returns a <see cref="ExecuteResult"/> instance that encapsulates
		/// the returned value of the function.
		/// </returns>
		/// <seealso cref="ExecuteResult.ReturnValue"/>
		public override ExecuteResult Execute(ExecuteContext context) {
			return context.FunctionResult(Evaluate(context.EvaluatedArguments));
		}

		/// <summary>
		/// When overridden in a derived class, this evaluates a function with
		/// the given parameters.
		/// </summary>
		/// <param name="args">The arguments of the function to execute.</param>
		/// <returns>
		/// Returns a <see cref="DataObject"/> that is the result of the execution
		/// of the function.
		/// </returns>
		protected virtual DataObject Evaluate(DataObject[] args) {
			return new DataObject(ReturnType(), SqlNull.Value);
		}

		/// <summary>
		/// Gets the function static return type
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="DataType"/> that defines
		/// the type of the returned value.
		/// </returns>
		public DataType ReturnType() {
			return ReturnType(null);
		}

		/// <summary>
		/// Resolves the function return type against the given context.
		/// </summary>
		/// <param name="context">The execution context used to resolve
		/// the function return type.</param>
		/// <returns>
		/// Returns an instance of <see cref="DataType"/> that defines
		/// the type of the returned value resolved against the given
		/// execution context..
		/// </returns>
		public virtual DataType ReturnType(ExecuteContext context) {
			return returnType;
		}

		#region DynamicType

		class DynamicDataType : DataType {
			public DynamicDataType()
				: base("DYNAMIC", SqlTypeCode.Object) {
			}

			public override bool IsComparable(DataType type) {
				return true;
			}

			public override int Compare(ISqlObject x, ISqlObject y) {
				throw new NotSupportedException();
			}
		}

		#endregion
	}
}