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

		public static bool IsSubQuery(this SqlExpressionType type) {
			return type.IsAny() || type.IsAll();
		}

		public static bool IsAll(this SqlExpressionType type) {
			return type == SqlExpressionType.AllEqual ||
			       type == SqlExpressionType.AllNotEqual ||
			       type == SqlExpressionType.AllGreaterThan ||
			       type == SqlExpressionType.AllSmallerThan ||
			       type == SqlExpressionType.AllGreaterOrEqualThan ||
			       type == SqlExpressionType.AllSmallerOrEqualThan;
		}

		public static bool IsAny(this SqlExpressionType type) {
			return type == SqlExpressionType.AnyEqual ||
			       type == SqlExpressionType.AnyNotEqual ||
			       type == SqlExpressionType.AnyGreaterThan ||
			       type == SqlExpressionType.AnySmallerThan ||
			       type == SqlExpressionType.AnyGreaterOrEqualThan ||
			       type == SqlExpressionType.AnySmallerOrEqualThan;
		}

		public static SqlExpressionType SubQueryPlainType(this SqlExpressionType type) {
			if (type.IsAny()) {
				if (type == SqlExpressionType.AnyEqual)
					return SqlExpressionType.Equal;
				if (type == SqlExpressionType.AnyNotEqual)
					return SqlExpressionType.NotEqual;
				if (type == SqlExpressionType.AnyGreaterThan)
					return SqlExpressionType.GreaterThan;
				if (type == SqlExpressionType.AnyGreaterOrEqualThan)
					return SqlExpressionType.GreaterOrEqualThan;
				if (type == SqlExpressionType.AnySmallerThan)
					return SqlExpressionType.AnySmallerThan;
				if (type == SqlExpressionType.AnySmallerOrEqualThan)
					return SqlExpressionType.SmallerOrEqualThan;
			}

			if (type.IsAll()) {
				if (type == SqlExpressionType.AllEqual)
					return SqlExpressionType.Equal;
				if (type == SqlExpressionType.AllNotEqual)
					return SqlExpressionType.NotEqual;
				if (type == SqlExpressionType.AllGreaterThan)
					return SqlExpressionType.GreaterThan;
				if (type == SqlExpressionType.AllGreaterOrEqualThan)
					return SqlExpressionType.GreaterOrEqualThan;
				if (type == SqlExpressionType.AllSmallerThan)
					return SqlExpressionType.AllSmallerThan;
				if (type == SqlExpressionType.AllSmallerOrEqualThan)
					return SqlExpressionType.SmallerOrEqualThan;
			}

			throw new ArgumentException();
		}

		public static SqlExpressionType Any(this SqlExpressionType type) {
			if (type == SqlExpressionType.Equal)
				return SqlExpressionType.AnyEqual;
			if (type == SqlExpressionType.NotEqual)
				return SqlExpressionType.AnyNotEqual;
			if (type == SqlExpressionType.GreaterThan)
				return SqlExpressionType.AnyGreaterThan;
			if (type == SqlExpressionType.GreaterOrEqualThan)
				return SqlExpressionType.AnyGreaterOrEqualThan;
			if (type == SqlExpressionType.SmallerThan)
				return SqlExpressionType.AnySmallerThan;
			if (type == SqlExpressionType.SmallerOrEqualThan)
				return SqlExpressionType.AnySmallerOrEqualThan;

			throw new ArgumentException();
		}

		public static SqlExpressionType All(this SqlExpressionType type) {
			if (type == SqlExpressionType.Equal)
				return SqlExpressionType.AllEqual;
			if (type == SqlExpressionType.NotEqual)
				return SqlExpressionType.AllNotEqual;
			if (type == SqlExpressionType.GreaterThan)
				return SqlExpressionType.AllGreaterThan;
			if (type == SqlExpressionType.GreaterOrEqualThan)
				return SqlExpressionType.AllGreaterOrEqualThan;
			if (type == SqlExpressionType.SmallerThan)
				return SqlExpressionType.AllSmallerThan;
			if (type == SqlExpressionType.SmallerOrEqualThan)
				return SqlExpressionType.AllSmallerOrEqualThan;

			throw new ArgumentException();
		}

		public static SqlExpressionType Inverse(this SqlExpressionType type) {
			if (type.IsSubQuery()) {
				var plainType = type.SubQueryPlainType();
				var invType = plainType.Inverse();

				if (type.IsAny())
					return invType.Any();
				if (type.IsAll())
					return invType.All();
			}

			switch (type) {
				case SqlExpressionType.Equal:
					return SqlExpressionType.NotEqual;
				case SqlExpressionType.NotEqual:
					return SqlExpressionType.Equal;
				case SqlExpressionType.GreaterThan:
					return SqlExpressionType.SmallerOrEqualThan;
				case SqlExpressionType.SmallerThan:
					return SqlExpressionType.GreaterOrEqualThan;
				case SqlExpressionType.GreaterOrEqualThan:
					return SqlExpressionType.SmallerThan;
				case SqlExpressionType.SmallerOrEqualThan:
					return SqlExpressionType.GreaterThan;
				case SqlExpressionType.And:
					return SqlExpressionType.Or;
				case SqlExpressionType.Or:
					return SqlExpressionType.And;
				case SqlExpressionType.Like:
					return SqlExpressionType.NotLike;
				case SqlExpressionType.NotLike:
					return SqlExpressionType.Like;
				case SqlExpressionType.Is:
					return SqlExpressionType.IsNot;
				case SqlExpressionType.IsNot:
					return SqlExpressionType.Is;
			}

			throw new ArgumentException();
		}
	}
}