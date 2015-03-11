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
using System.Linq;
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	public sealed class FunctionInfo : RoutineInfo {
		public FunctionInfo(ObjectName name) 
			: base(name) {
			AssertUnboundAtEnd();
		}

		public FunctionInfo(ObjectName name, RoutineParameter[] parameters) 
			: base(name, parameters) {
			AssertUnboundAtEnd();
		}

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

		public FunctionType FunctionType { get; internal set; }

		internal override bool MatchesInvoke(RoutineInvoke invoke, IQueryContext queryContext) {
			if (invoke == null)
				return false;

			// TODO: have a patch to check if this must be case-insensitive compare
			// TODO: have the invoke to respect the [Name1].[Name2].[NameN] format as the routine
			if (!Name.Equals(invoke.RoutineName))
				return false;

			// TODO: add a better resolution to obtain the final type of the argument
			//       and compare it to the parameter type definition
			bool unboundedSeen = false;
			for (int i = 0; i < invoke.Arguments.Length; i++) {
				if (i + 1 > Parameters.Length) {
					if (!unboundedSeen)
						return false;

					// TODO: verify the type of the argument (how to evaluate?)

				} else {
					var param = Parameters[i];
					unboundedSeen = param.IsUnbounded;

					// TODO: verify the type of the argument (how to evaluate?)
				}
			}

			if (!unboundedSeen && invoke.Arguments.Length != Parameters.Length)
				return false;

			return true;
		}
	}
}