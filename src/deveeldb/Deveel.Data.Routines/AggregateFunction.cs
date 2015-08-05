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
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Types;

namespace Deveel.Data.Routines {
	public abstract class AggregateFunction : Function {
		protected AggregateFunction(ObjectName name, RoutineParameter[] parameters, DataType returnType) 
			: base(name, parameters, returnType, FunctionType.Aggregate) {
		}

		protected abstract DataObject Evaluate(DataObject value1, DataObject value2, IQueryContext context, IGroupResolver group);

		protected virtual DataObject PostEvaluate(DataObject result, IQueryContext context, IGroupResolver group) {
			// By default, do nothing....
			return result;
		}

		public override ExecuteResult Execute(ExecuteContext context) {
			if (context == null)
				throw new ArgumentNullException("context");

			var group = context.GroupResolver;

			if (group == null)
				throw new Exception(String.Format("'{0}' can only be used as an aggregate function.", FunctionName));

			DataObject result = null;
			// All aggregates functions return 'null' if group size is 0
			int size = group.Count;
			if (size == 0) {
				// Return a NULL of the return type
				return context.Result(DataObject.Null(ReturnType(context)));
			}

			DataObject val;
			ObjectName v = context.Arguments[0].AsReferenceName();
			// If the aggregate parameter is a simple variable, then use optimal
			// routine,
			if (v != null) {
				for (int i = 0; i < size; ++i) {
					val = group.Resolve(v, i);
					result = Evaluate(result, val, context.QueryContext, group);
				}
			} else {
				// Otherwise we must resolve the expression for each entry in group,
				// This allows for expressions such as 'sum(quantity * price)' to
				// work for a group.
				var exp = context.Arguments[0];
				for (int i = 0; i < size; ++i) {
					val = exp.EvaluateToConstant(context.QueryContext, group.GetVariableResolver(i));
					result = Evaluate(result, val, context.QueryContext, group);
				}
			}

			// Post method.
			result = PostEvaluate(result, context.QueryContext, group);

			return context.Result(result);
		}
	}
}
