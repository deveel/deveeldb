// 
//  Copyright 2010-2014 Deveel
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

using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	public abstract class Function : Routine {
		public static readonly TType DynamicType = new TDynamicType();

		protected Function(RoutineName name, RoutineParameter[] parameters) 
			: this(name, parameters, null) {
		}

		protected Function(RoutineName name, RoutineParameter[] parameters, TType returnType) 
			: base(name, parameters, RoutineType.Function) {
			this.returnType = returnType;
		}

		private readonly TType returnType;

		public override ExecuteResult Execute(ExecuteContext context) {
			return context.FunctionResult(Evaluate(context.EvaluatedArguments));
		}

		protected virtual TObject Evaluate(TObject[] args) {
			return new TObject(ReturnType(), null);
		}

		public TType ReturnType() {
			return ReturnType(null);
		}

		public virtual TType ReturnType(ExecuteContext context) {
			return returnType;
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