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
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Variables {
	public static class QueryExtensions {
		public static Variable DeclareVariable(this IQuery query, VariableInfo variableInfo) {
			return query.Context.DeclareVariable(variableInfo);
		}

		public static void DropVariable(this IQuery query, string variableName) {
			query.Context.DropVariable(variableName);
		}

		public static Variable SetVariable(this IQuery query, string variableName, SqlExpression value) {
			return query.Context.SetVariable(variableName, value);
		}

		public static Variable FindVariable(this IQuery query, string variableName) {
			return query.Context.FindVariable(variableName);
		}

		public static Variable DeclareVariable(this IQuery query, string variableName, SqlType variableType) {
			return DeclareVariable(query, variableName, variableType, false);
		}

		public static Variable DeclareVariable(this IQuery query, string variableName, SqlType variableType, bool constant) {
			return query.DeclareVariable(new VariableInfo(variableName, variableType, constant));
		}
	}
}
