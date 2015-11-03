﻿using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Variables {
	public static class QueryContextExtensions {
		public static Variable DeclareVariable(this IQueryContext context, VariableInfo variableInfo) {
			return context.VariableManager.DefineVariable(variableInfo);
		}

		public static Variable DeclareVariable(this IQueryContext context, string name, SqlType type) {
			return DeclareVariable(context, name, type, null);
		}

		public static Variable DeclareVariable(this IQueryContext context, string name, SqlType type, SqlExpression defaultExpression) {
			return DeclareVariable(context, name, type, false, defaultExpression);
		}

		public static Variable DeclareVariable(this IQueryContext context, string name, SqlType type, bool notNull) {
			return DeclareVariable(context, name, type, notNull, null);
		}

		public static Variable DeclareVariable(this IQueryContext context, string name, SqlType type, bool notNull, SqlExpression defaultExpression) {
			var info = new VariableInfo(name, type, false);
			info.DefaultExpression = defaultExpression;
			info.IsNotNull = notNull;

			return context.VariableManager.DefineVariable(info);
		}

		public static void DeclareConstantVariable(this IQueryContext context, string name, SqlType type, SqlExpression defaultExpression) {
			if (defaultExpression == null)
				throw new ArgumentNullException("defaultExpression");
			if (type == null)
				throw new ArgumentNullException("type");

			// TODO: Create a variable resolver for the query context scope
			var expType = defaultExpression.ReturnType(context,  null);
			if (!type.IsComparable(expType))
				throw new ArgumentException();

			var info = new VariableInfo(name, type, true);
			info.DefaultExpression = defaultExpression;
			info.IsNotNull = true;

			context.DeclareVariable(info);
		}

		public static Variable FindVariable(this IQueryContext context, string name) {
			var variable = context.VariableManager.GetVariable(name);
			if (variable == null &&
			    context.ParentContext != null)
				variable = context.ParentContext.FindVariable(name);

			return variable;
		}

		public static void SetVariable(this IQueryContext context, string name, SqlExpression expression) {
			var variable = context.FindVariable(name);
			if (variable == null && expression != null) {
				// TODO: Create a variable resolver for the query context scope
				var varType = expression.ReturnType(context, null);
				variable = context.DeclareVariable(name, varType);
			}

			if (variable == null)
				throw new ObjectNotFoundException(new ObjectName(name));

			variable.SetValue(context, expression);
		}
	}
}
