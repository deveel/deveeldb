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
using System.Linq.Expressions;
using System.Text;

using Deveel.Data.DbSystem;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Routines {
	public sealed class RoutineInvoke {
		private IRoutine cached;

		public RoutineInvoke(ObjectName routineName, SqlExpression[] arguments) {
			RoutineName = routineName;
			Arguments = arguments;
		}

		public ObjectName RoutineName { get; private set; }

		public SqlExpression[] Arguments { get; private set; }

		public bool IsGlobArgument {
			get {
				return Arguments != null &&
				       Arguments.Length == 1 &&
				       Arguments[0] is SqlConstantExpression &&
				       ((SqlConstantExpression) Arguments[0]).Value.Value.ToString() == "*";
			}
		}

		public bool IsAggregate(IQueryContext context) {
			var resolver = context.RoutineResolver;
			if (resolver.IsAggregateFunction(this, context))
				return true;

			// Look at parameterss
			return Arguments.Any(x => x.HasAggregate(context));
		}

		public IRoutine GetRoutine(IQueryContext context) {
			if (cached != null)
				return cached;

			IRoutineResolver resolver;
			if (context == null) {
				resolver = SystemFunctions.Factory;
			} else {
				resolver = context.RoutineResolver;
			}

			cached = resolver.ResolveRoutine(this, context);
			if (cached == null)
				throw new InvalidOperationException(String.Format("Unable to resolve the call {0} to a function", this));

			return cached;
		}

		public override String ToString() {
			var buf = new StringBuilder();
			buf.Append(RoutineName);
			buf.Append('(');
			for (int i = 0; i < Arguments.Length; ++i) {
				buf.Append(Arguments[i]);
				if (i < Arguments.Length - 1) {
					buf.Append(',');
				}
			}
			buf.Append(')');
			return buf.ToString();
		}
	}
}
