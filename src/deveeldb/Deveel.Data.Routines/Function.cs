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
	public abstract class Function : Routine {
		public static readonly DataType DynamicType = new DynamicDataType();

		protected Function(ObjectName name, RoutineParameter[] parameters) 
			: this(name, parameters, null) {
		}

		protected Function(ObjectName name, RoutineParameter[] parameters, DataType returnType) 
			: base(name, parameters, RoutineType.Function) {
			this.returnType = returnType;
		}

		private readonly DataType returnType;

		public override ExecuteResult Execute(ExecuteContext context) {
			return context.FunctionResult(Evaluate(context.EvaluatedArguments));
		}

		protected virtual DataObject Evaluate(DataObject[] args) {
			return new DataObject(ReturnType(), null);
		}

		public DataType ReturnType() {
			return ReturnType(null);
		}

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