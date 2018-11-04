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
using System.Runtime.Serialization;

namespace Deveel.Data.Sql.Types {
	/// <summary>
	/// Defines the type of an one-dimensions array of SQL expressions
	/// </summary>
	/// <remarks>
	/// <para>
	/// Arrays have a fixed length of elements and they are
	/// immutable once defined.
	/// </para>
	/// <para>
	/// It is possible to access the content of arrays through
	/// zero-based indices
	/// </para>
	/// </remarks>
	/// <seealso cref="SqlArray"/>
	[Serializable]
	public sealed class SqlArrayType : SqlType {
		/// <summary>
		/// Constructs an array type with the given size
		/// </summary>
		/// <param name="length">The length of the array</param>
		/// <exception cref="ArgumentException">
		/// Thrown if the <paramref name="length"/> is smaller than zero
		/// </exception>
		public SqlArrayType(int length)
			: base(SqlTypeCode.Array) {
			if (length < 0)
				throw new ArgumentException("Invalid array length");

			Length = length;
		}

		private SqlArrayType(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			Length = info.GetInt32("length");
		}

		/// <summary>
		/// Gets the length of the array
		/// </summary>
		public int Length { get; }

		public override bool IsInstanceOf(ISqlValue value) {
			return value is SqlArray && ((SqlArray) value).Length == Length;
		}

		public override bool Equals(SqlType other) {
			if (!(other is SqlArrayType))
				return false;

			var otherType = (SqlArrayType) other;
			return Length == otherType.Length;
		}

		protected override void AppendTo(SqlStringBuilder sqlBuilder) {
			sqlBuilder.Append($"ARRAY({Length})");
		}

		protected override void GetObjectData(SerializationInfo info) {
			info.AddValue("length", Length);
		}
	}
}