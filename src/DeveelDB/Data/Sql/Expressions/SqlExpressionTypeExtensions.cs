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

namespace Deveel.Data.Sql.Expressions {
	static class SqlExpressionTypeExtensions {
		public static bool IsBinary(this SqlExpressionType expressionType) {
			switch (expressionType) {
				case SqlExpressionType.Add:
				case SqlExpressionType.Subtract:
				case SqlExpressionType.Divide:
				case SqlExpressionType.Multiply:
				case SqlExpressionType.Modulo:
				case SqlExpressionType.And:
				case SqlExpressionType.Or:
				case SqlExpressionType.XOr:
				case SqlExpressionType.Equal:
				case SqlExpressionType.NotEqual:
				case SqlExpressionType.GreaterThan:
				case SqlExpressionType.GreaterThanOrEqual:
				case SqlExpressionType.LessThan:
				case SqlExpressionType.LessThanOrEqual:
				case SqlExpressionType.Is:
				case SqlExpressionType.IsNot:
					return true;
				default:
					return false;
			}
		}

		public static bool IsRelational(this SqlExpressionType expressionType) {
			switch (expressionType) {
				case SqlExpressionType.Equal:
				case SqlExpressionType.NotEqual:
				case SqlExpressionType.GreaterThan:
				case SqlExpressionType.LessThan:
				case SqlExpressionType.GreaterThanOrEqual:
				case SqlExpressionType.LessThanOrEqual:
				case SqlExpressionType.Is:
				case SqlExpressionType.IsNot:
					return true;
				default:
					return false;
			}
		}

		public static bool IsUnary(this SqlExpressionType expressionType) {
			switch (expressionType) {
				case SqlExpressionType.UnaryPlus:
				case SqlExpressionType.Not:
				case SqlExpressionType.Negate:
					return true;
				default:
					return false;
			}
		}

		public static bool IsQuantify(this SqlExpressionType expressionType) {
			switch (expressionType) {
				case SqlExpressionType.All:
				case SqlExpressionType.Any:
					return true;
				default:
					return false;
			}
		}
	}
}