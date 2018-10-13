// 
//  Copyright 2010-2018 Deveel
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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Variables {
	public static class ContextExtensions {
		public static Variable ResolveVariable(this IContext context, string name) {
			var ignoreCase = context.IgnoreCase();

			var current = context;
			while (current != null) {
				IVariableResolver resolver = null;

				if (current is IVariableScope) {
					var scope = (IVariableScope) current;
					resolver = scope.Variables;
				} else if (current is IVariableResolver) {
					resolver = (IVariableResolver) current;
				}

				if (resolver != null) {
					var variable = resolver.ResolveVariable(name, ignoreCase);
					if (variable != null)
						return variable;
				}


				current = current.ParentContext;
			}

			return null;
		}

		public static TManager GetVariableManager<TManager>(this IContext context)
			where TManager : class, IVariableManager {
			return context.GetObjectManager<TManager>(DbObjectType.Variable);
		}

		public static SqlType ResolveVariableType(this IContext context, string name) {
			var ignoreCase = context.IgnoreCase();

			var current = context;
			while (current != null) {
				IVariableResolver resolver = null;

				if (current is IVariableScope) {
					var scope = (IVariableScope)current;
					resolver = scope.Variables;
				} else if (current is IVariableResolver) {
					resolver = (IVariableResolver)current;
				}

				if (resolver != null) {
					var type = resolver.ResolveVariableType(name, ignoreCase);
					if (type != null)
						return type;
				}


				current = current.ParentContext;
			}

			return null;
		}

		public static SqlExpression AssignVariable(this QueryContext context, string name, SqlExpression value) {
			var ignoreCase = context.IgnoreCase();

			IContext current = context;
			while (current != null) {
				IVariableResolver resolver = null;

				if (current is IVariableScope) {
					var scope = (IVariableScope)current;
					resolver = scope.Variables;
				} else if (current is IVariableResolver) {
					resolver = (IVariableResolver)current;
				}

				if (resolver != null) {
					var variable = resolver.ResolveVariable(name, ignoreCase);
					if (variable == null) {
						if (resolver is IVariableManager) {
							var manager = (IVariableManager) resolver;

							// here we pass the root context for resolving innermost references
							return manager.AssignVariable(context, name, ignoreCase, value);
						}
					} else {
						return variable.SetValue(value, context);
					}
				}


				current = current.ParentContext;
			}

			throw new SqlExpressionException($"Could not find variable '{name}' in the context hierarchy and no variable manager was found");
		}
	}
}