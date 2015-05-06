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

using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	public sealed class UserFunction : Function {
		public UserFunction(FunctionInfo functionInfo) 
			: base(functionInfo) {
		}

		public UserFunction(ObjectName name, RoutineParameter[] parameters, FunctionType functionType) 
			: base(name, parameters, functionType) {
		}

		public UserFunction(ObjectName name, RoutineParameter[] parameters, DataType returnType) 
			: base(name, parameters, returnType) {
		}

		public UserFunction(ObjectName name, RoutineParameter[] parameters, DataType returnType, FunctionType functionType) 
			: base(name, parameters, returnType, functionType) {
		}

		protected override DataObject Evaluate(DataObject[] args) {
			throw new NotImplementedException();
		}
	}
}
