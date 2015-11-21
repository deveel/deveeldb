using System;

using Deveel.Data.Services;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Types;

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

			public DataObject Resolve(ObjectName variableName) {
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
