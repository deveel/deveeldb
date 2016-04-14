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

namespace Deveel.Data.Sql.Expressions {
	internal static class SqlExpressionTypeExtensions {
		public static SqlExpressionType Reverse(this SqlExpressionType type) {
			if (type == SqlExpressionType.Equal ||
			    type == SqlExpressionType.NotEqual ||
			    type == SqlExpressionType.Is ||
			    type == SqlExpressionType.IsNot)
				return type;
			if (type == SqlExpressionType.GreaterThan)
				return SqlExpressionType.SmallerThan;
			if (type == SqlExpressionType.SmallerThan)
				return SqlExpressionType.GreaterThan;
			if (type == SqlExpressionType.GreaterOrEqualThan)
				return SqlExpressionType.SmallerOrEqualThan;
			if (type == SqlExpressionType.SmallerOrEqualThan)
				return SqlExpressionType.GreaterOrEqualThan;

			throw new InvalidOperationException("Cannot reverse a non conditional operator.");
		}

		public static bool IsLogical(this SqlExpressionType type) {
			return type == SqlExpressionType.And ||
			       type == SqlExpressionType.Or;
		}

		public static bool IsPattern(this SqlExpressionType type) {
			return type == SqlExpressionType.Like ||
			       type == SqlExpressionType.NotLike;
		}

	}
}