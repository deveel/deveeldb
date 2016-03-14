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

using Deveel.Data;
using Deveel.Data.Security;
using Deveel.Data.Sql;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Routines {
	public static class RoutineExtensions {
		public static InvokeResult Execute(this IRoutine routine) {
			return Execute(routine, new SqlExpression[0]);
		}

		public static InvokeResult Execute(this IRoutine routine, SqlExpression[] args) {
			return Execute(routine, args, null);
		}

		public static InvokeResult Execute(this IRoutine routine, SqlExpression[] args, IQuery context) {
			return Execute(routine, args, context, null);
		}

		public static InvokeResult Execute(this IRoutine routine, SqlExpression[] args, IQuery query, IVariableResolver resolver) {
			return Execute(routine, args, query, resolver, null);
		}

		public static InvokeResult Execute(this IRoutine routine, IQuery query) {
			return Execute(routine, query, null);
		}

		public static InvokeResult Execute(this IRoutine routine, IQuery query, IVariableResolver resolver) {
			return Execute(routine, query, resolver, null);
		}

		public static InvokeResult Execute(this IRoutine routine, IQuery query, IVariableResolver resolver, IGroupResolver group) {
			return Execute(routine, new SqlExpression[0], query, resolver, group);
		}

		public static InvokeResult Execute(this IRoutine routine, SqlExpression[] args, IQuery query, IVariableResolver resolver, IGroupResolver group) {
			var request = new Invoke(routine.FullName, args);

			if (query != null &&
			    !query.Session.SystemAccess.UserCanExecuteFunction(request, query))
				throw new InvalidOperationException();

			var executeContext = new InvokeContext(request, routine, resolver, group, query);
			return routine.Execute(executeContext);
		}

		public static InvokeResult Execute(this IRoutine routine, Field[] args) {
			var exps = new SqlExpression[0];
			if (args != null && args.Length > 0) {
				exps = new SqlExpression[args.Length];

				for (int i = 0; i < args.Length; i++) {
					exps[i] = SqlExpression.Constant(args[i]);
				}
			}

			return routine.Execute(exps);
		}
	}
}