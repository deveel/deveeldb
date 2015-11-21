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

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Variables {
	public static class QueryContextExtensions {
		//public static void DeclareConstantVariable(this IQueryContext context, string name, SqlType type, SqlExpression defaultExpression) {
		//	if (defaultExpression == null)
		//		throw new ArgumentNullException("defaultExpression");
		//	if (type == null)
		//		throw new ArgumentNullException("type");

		//	// TODO: Create a variable resolver for the query context scope
		//	var expType = defaultExpression.ReturnType(context,  null);
		//	if (!type.IsComparable(expType))
		//		throw new ArgumentException();

		//	var info = new VariableInfo(name, type, true);
		//	info.DefaultExpression = defaultExpression;
		//	info.IsNotNull = true;

		//	context.DeclareVariable(info);
		//}

		//public static Variable FindVariable(this IQueryContext context, string name) {
		//	var variable = context.VariableManager.GetVariable(name);
		//	if (variable == null &&
		//	    context.ParentContext != null)
		//		variable = context.ParentContext.FindVariable(name);

		//	return variable;
		//}

		//public static void SetVariable(this IQueryContext context, string name, SqlExpression expression) {
		//	var variable = context.FindVariable(name);
		//	if (variable == null && expression != null) {
		//		// TODO: Create a variable resolver for the query context scope
		//		var varType = expression.ReturnType(context, null);
		//		variable = context.DeclareVariable(name, varType);
		//	}

		//	if (variable == null)
		//		throw new ObjectNotFoundException(new ObjectName(name));

		//	variable.SetValue(context, expression);
		//}
	}
}
