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

using Deveel.Data.Sql;
using Deveel.Data.Sql.Types;

using ISqlObject = Deveel.Data.Sql.Objects.ISqlObject;

namespace Deveel.Data.Routines {
	/// <summary>
	/// A system routine that returns a value at the end of its execution. 
	/// </summary>
	/// <remarks>
	/// This class provides the base features for constructing functions.
	/// </remarks>
	public abstract class Function : IFunction {
		/// <summary>
		/// A special <see cref="SqlType"/> that is used to mark an argument
		/// of a function as <c>dynamic</c>.
		/// </summary>
		/// <remarks>
		/// This <see cref="SqlType"/> matches against any passed object.
		/// </remarks>
		public static readonly SqlType DynamicType = new DynamicSqlType();

		protected Function(FunctionInfo functionInfo) {
			if (functionInfo == null)
				throw new ArgumentNullException("functionInfo");

			FunctionInfo = functionInfo;
		}

		protected Function(ObjectName name, RoutineParameter[] parameters, FunctionType functionType) 
			: this(name, parameters, null, functionType) {
		}

		protected Function(ObjectName name, RoutineParameter[] parameters, SqlType returnType) 
			: this(name, parameters, returnType, FunctionType.Static) {
		}

		protected Function(ObjectName name, RoutineParameter[] parameters, SqlType returnType, FunctionType functionType)
			: this(new FunctionInfo(name, parameters, returnType, functionType)) {
		}

		public FunctionType FunctionType {
			get { return FunctionInfo.FunctionType; }
		}

		public FunctionInfo FunctionInfo { get; private set; }

		public ObjectName FunctionName {
			get { return FunctionInfo.RoutineName; }
		}

		RoutineInfo IRoutine.RoutineInfo {
			get { return FunctionInfo; }
		}

		RoutineType IRoutine.Type {
			get { return RoutineType.Function; }
		}

		DbObjectType IDbObject.ObjectType {
			get { return DbObjectType.Routine; }
		}

		ObjectName IDbObject.FullName {
			get { return FunctionName; }
		}

		/// <summary>
		/// Executes the function and provides a result.
		/// </summary>
		/// <param name="context">The context of the execution.</param>
		/// <returns>
		/// Returns a <see cref="InvokeResult"/> instance that encapsulates
		/// the returned value of the function.
		/// </returns>
		/// <seealso cref="InvokeResult.ReturnValue"/>
		public abstract InvokeResult Execute(InvokeContext context);

		/// <summary>
		/// Gets the function static return type
		/// </summary>
		/// <returns>
		/// Returns an instance of <see cref="SqlType"/> that defines
		/// the type of the returned value.
		/// </returns>
		public SqlType ReturnType() {
			if (FunctionInfo.ReturnType != null)
				return FunctionInfo.ReturnType;

			return ReturnType(null);
		}

		/// <summary>
		/// Resolves the function return type against the given context.
		/// </summary>
		/// <param name="context">The execution context used to resolve
		/// the function return type.</param>
		/// <returns>
		/// Returns an instance of <see cref="SqlType"/> that defines
		/// the type of the returned value resolved against the given
		/// execution context..
		/// </returns>
		public virtual SqlType ReturnType(InvokeContext context) {
			return FunctionInfo.ReturnType;
		}

		#region DynamicType

		class DynamicSqlType : SqlType {
			public DynamicSqlType()
				: base("DYNAMIC", SqlTypeCode.Object) {
			}

			public override bool IsComparable(SqlType type) {
				return true;
			}

			public override int Compare(ISqlObject x, ISqlObject y) {
				throw new NotSupportedException();
			}
		}

		#endregion
	}
}