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

namespace Deveel.Data.Deveel.Data.Sql {
	public enum BinaryOperatorType {
		Divide,
		Multiply,
		Modulo,

		Add,
		Subtract,

		/// <summary>
		/// The values are equal.
		/// </summary>
		Equal,

		/// <summary>
		/// The values are not equal.
		/// </summary>
		NotEqual,

		/// <summary>
		/// The first value is greater than the second.
		/// </summary>
		GreaterThan,

		/// <summary>
		/// The first value is smaller than the second.
		/// </summary>
		SmallerThan,

		/// <summary>
		/// The first value is greater or equal to the second.
		/// </summary>
		GreaterOrEqualThan,

		/// <summary>
		/// The first value is smaller or equal to the second.
		/// </summary>
		SmallerOrEqualThan,

		/// <summary>
		/// The first string value is like the second string.
		/// </summary>
		Like,

		/// <summary>
		/// The first string value is not like the second string.
		/// </summary>
		NotLike,

		/// <summary>
		/// The first value is equivalent to the second value.
		/// </summary>
		Is,

		/// <summary>
		/// The first value is not equivalent to the second value.
		/// </summary>
		IsNot,

		/// <summary>
		/// The values are all true
		/// </summary>
		And,

		/// <summary>
		/// Either one of the values is true
		/// </summary>
		Or,
		XOr
	}
}