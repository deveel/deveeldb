// 
//  Copyright 2010-2016 Deveel
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

namespace Deveel.Data.Sql.Expressions.Build {
	public interface IExpressionBuilder {
		IExpressionBuilder Value(object value);

		IExpressionBuilder Binary(SqlExpressionType binaryType, Action<IExpressionBuilder> right);

		IExpressionBuilder Unary(SqlExpressionType unaryType);

		IExpressionBuilder Query(Action<IQueryExpressionBuilder> query);

		IExpressionBuilder Function(ObjectName functionName, params SqlExpression[] args);

		IExpressionBuilder Reference(ObjectName referenceName);

		IExpressionBuilder Variable(string variableName);

		IExpressionBuilder Quantified(SqlExpressionType quantifyType, Action<IExpressionBuilder> expression);
	}
}
