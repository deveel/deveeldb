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

using Deveel.Data.Services;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Variables {
	public static class ContextExtensions {
		public static Variable FindVariable(this IContext context, string variableName) {
			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is IVariableScope) {
					var scope = (IVariableScope) currentContext;
					var variable = scope.GetVariable(variableName);
					if (variable != null)
						return variable;
				}

				currentContext = currentContext.Parent;
			}


			// not found in the hierarchy
			return null;
		}

		public static bool VariableExists(this IContext context, string variableName) {
			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is IVariableScope) {
					var scope = (IVariableScope)currentContext;
					if (scope.HasVariable(variableName))
						return true;
				}

				currentContext = currentContext.Parent;
			}


			// not found in the hierarchy
			return false;
		}

		public static bool DropVariable(this IContext context, string variableName) {
			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is IVariableScope) {
					var scope = (IVariableScope)currentContext;
					if (scope.HasVariable(variableName))
						return scope.DropVariable(variableName);
				}

				currentContext = currentContext.Parent;
			}


			// not found in the hierarchy
			return false;
		}

		public static Variable DeclareVariable(this IContext context, VariableInfo variableInfo) {
			if (context.VariableExists(variableInfo.VariableName))
				throw new InvalidOperationException(String.Format("Variable '{0}' already defined in the context hierarchy.", variableInfo.VariableName));

			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is IVariableScope) {
					var scope = (IVariableScope)currentContext;
					return scope.DefineVariable(variableInfo);
				}

				currentContext = currentContext.Parent;
			}

			// not found in the hierarchy
			return null;
		}

		public static Variable DeclareVariable(this IContext context, string variableName, SqlType variableType) {
			return DeclareVariable(context, variableName, variableType, false);
		}

		public static Variable DeclareVariable(this IContext context, string variableName, SqlType variableType, bool constant) {
			return context.DeclareVariable(new VariableInfo(variableName, variableType, constant));
		}

		public static Variable SetVariable(this IContext context, string variableName, SqlExpression value) {
			var currentContext = context;
			while (currentContext != null) {
				if (currentContext is IVariableScope) {
					var scope = (IVariableScope) currentContext;
					if (scope.HasVariable(variableName)) {
						// TODO: support also in-context evaluation
						var constantValue = value.EvaluateToConstant(null, context.VariableResolver());
						return scope.SetVariable(variableName, constantValue);
					}
				}

				currentContext = currentContext.Parent;
			}

			currentContext = context;
			while (currentContext != null) {
				if (currentContext is IVariableScope) {
					var scope = (IVariableScope)currentContext;
						// TODO: support also in-context evaluation
					var constantValue = value.EvaluateToConstant(null, context.VariableResolver());
					return scope.SetVariable(variableName, constantValue);
				}

				currentContext = currentContext.Parent;
			}

			// not found in the hierarchy
			return null;
		}

		public static IVariableResolver VariableResolver(this IContext context) {
			return new ContextVariableResolver(context);
		}

		#region ContextVariableResolver

		class ContextVariableResolver : IVariableResolver, IDisposable {
			private IContext context;

			public ContextVariableResolver(IContext context) {
				this.context = context;
			}

			public Field Resolve(ObjectName variableName) {
				var variable = context.FindVariable(variableName.Name);
				if (variable == null)
					return null;

				return variable.Value;
			}

			public SqlType ReturnType(ObjectName variableName) {
				var variable = context.FindVariable(variableName.Name);
				if (variable == null)
					return null;

				return variable.Type;
			}

			public void Dispose() {
				context = null;
			}
		}

		#endregion
	}
}
