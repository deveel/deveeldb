using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Variables {
	public static class VariableScopeExtensions {
		public static bool HasVariable(this IVariableScope scope, string variableName) {
			return scope.VariableManager.VariableExists(variableName);
		}

		public static Variable DefineVariable(this IVariableScope scope, VariableInfo variableInfo) {
			return scope.VariableManager.DefineVariable(variableInfo);
		}

		public static Variable DefineVariable(this IVariableScope scope, string variableName, SqlType variableType) {
			return DefineVariable(scope, variableName, variableType, false);
		}

		public static Variable DefineVariable(this IVariableScope scope, string variableName, SqlType variableType, bool constant) {
			return scope.DefineVariable(new VariableInfo(variableName, variableType, constant));
		}

		public static bool DropVariable(this IVariableScope scope, string variableName) {
			return scope.VariableManager.DropVariable(variableName);
		}

		public static Variable GetVariable(this IVariableScope scope, string variableName) {
			return scope.VariableManager.GetVariable(variableName);
		}

		public static Variable SetVariable(this IVariableScope scope, string variableName, DataObject value) {
			var variable = scope.GetVariable(variableName);
			if (variable == null)
				variable = scope.DefineVariable(variableName, value.Type);

			variable.SetValue(value);
			return variable;
		}

		public static void SetBooleanVariable(this IVariableScope transaction, string name, bool value) {
			transaction.SetVariable(name, DataObject.Boolean(value));
		}

		public static void SetStringVariable(this IVariableScope transaction, string name, string value) {
			transaction.SetVariable(name, DataObject.String(value));
		}

		public static bool GetBooleanVariable(this IVariableScope transaction, string name) {
			var variable = transaction.GetVariable(name);
			if (variable == null)
				return false;

			return variable.Value.AsBoolean();
		}

		public static string GetStringVariable(this IVariableScope transaction, string name) {
			var variable = transaction.GetVariable(name);
			if (variable == null)
				return null;

			return variable.Value;
		}
	}
}
