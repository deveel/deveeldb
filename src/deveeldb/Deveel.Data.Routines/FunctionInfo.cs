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
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Routines {
	/// <summary>
	/// The function signature information that are used to resolve
	/// a function within a context.
	/// </summary>
	/// <seealso cref="RoutineInfo"/>
	public abstract class FunctionInfo : RoutineInfo {
		public FunctionInfo(ObjectName routineName, SqlType returnType, FunctionType functionType) 
			: base(routineName) {
			ReturnType = returnType;
			FunctionType = functionType;
			AssertUnboundAtEnd();
		}

		public FunctionInfo(ObjectName routineName, RoutineParameter[] parameters, SqlType returnType, FunctionType functionType) 
			: base(routineName, parameters) {
			ReturnType = returnType;
			FunctionType = functionType;
			AssertUnboundAtEnd();
		}

		public override RoutineType RoutineType {
			get { return RoutineType.Function; }
		}

		public SqlType ReturnType { get; private set; }

		private void AssertUnboundAtEnd() {
			for (int i = 0; i < Parameters.Length; i++) {
				var param = Parameters[i];
				if (param.IsUnbounded) {
					if (HasUnboundParameter)
						throw new ArgumentException("Cannot specify more than one 'unbounded' argument for a function.");

					if (i != Parameters.Length - 1)
						throw new ArgumentException("An unbounded parameter must be present only at the end.");

					HasUnboundParameter = true;
				}
			}
		}

		private bool HasUnboundParameter { get; set; }

		/// <summary>
		/// Gets the kind of function.
		/// </summary>
		public FunctionType FunctionType { get; private set; }

		internal override bool MatchesInvoke(Invoke invoke, IRequest request) {
			if (invoke == null)
				return false;

			bool ignoreCase = true;
			if (request != null)
				ignoreCase = request.Query.IgnoreIdentifiersCase();

			if (!RoutineName.Equals(invoke.RoutineName, ignoreCase))
				return false;

			// TODO: add a better resolution to obtain the final type of the argument
			//       and compare it to the parameter type definition
			bool unboundedSeen = false;
			for (int i = 0; i < invoke.Arguments.Length; i++) {
				var argType = invoke.Arguments[i].ReturnType(request, null);

				if (i + 1 > Parameters.Length) {
					if (!unboundedSeen)
						return false;

					// TODO: verify the type of the argument (how to evaluate?)

				} else {
					var param = Parameters[i];
					unboundedSeen = param.IsUnbounded;

					var paramType = param.Type;

					if (!paramType.IsComparable(argType))
						return false;
				}
			}

			if (!unboundedSeen &&
				invoke.Arguments.Length != Parameters.Length)
				return false;

			return true;
		}
	}
}