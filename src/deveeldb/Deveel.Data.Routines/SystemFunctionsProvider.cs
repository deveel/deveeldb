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

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Fluid;

namespace Deveel.Data.Routines {
	class SystemFunctionsProvider : FunctionProvider {
		public override string SchemaName {
			get { return SystemSchema.Name; }
		}

		protected override ObjectName NormalizeName(ObjectName functionName) {
			if (functionName.Parent == null)
				return new ObjectName(new ObjectName(SchemaName), functionName.Name);

			return base.NormalizeName(functionName);
		}

		private ExecuteResult Simple(ExecuteContext context, Func<DataObject[], DataObject> func) {
			var evaluated = context.EvaluatedArguments;
			var value = func(evaluated);
			return context.Result(value);
		}

		private ExecuteResult Simple(ExecuteContext context, Func<DataObject, DataObject, DataObject> func) {
			var evaluated = context.EvaluatedArguments;
			var value = func(evaluated[0], evaluated[1]);
			return context.Result(value);			
		}

		private void AddAggregateFunctions() {
			// Aggregate OR
			New("aggor")
				.WithParameter(p => p.Named("args").Unbounded().OfDynamicType())
				.Aggregate()
				.WhenExecute(context => Simple(context, SystemFunctions.Or));
		}

		private void AddSecurityFunctions() {
			New("user")
				.WhenExecute(context => context.Result(SystemFunctions.User(context.QueryContext)))
				.ReturnsString();
		}

		protected override void OnInit() {
			AddAggregateFunctions();

			AddSecurityFunctions();
		}
	}
}
