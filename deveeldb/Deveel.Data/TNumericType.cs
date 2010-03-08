// 
//  Copyright 2010  Deveel
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

using System;

using Deveel.Math;

namespace Deveel.Data {
	///<summary>
	/// An implementation of <see cref="TType"/> for a number.
	///</summary>
	[Serializable]
	public sealed class TNumericType : TType {
		/// <summary>
		/// The size of the number.
		/// </summary>
		private int size;

		/// <summary>
		/// The scale of the number.
		/// </summary>
		private int scale;


		///<summary>
		/// Constructs a type with the given sql_type value, the size,
		/// and the scale of the number.
		///</summary>
		///<param name="sql_type">A valid <c>NUMERIC</c> SQL type.</param>
		///<param name="size">The size of the type (if any, -1 otherwise).</param>
		///<param name="scale">The scale of the numberic type (or -1 if not 
		/// specified).</param>
		public TNumericType(SqlType sql_type, int size, int scale)
			: base(sql_type) {
			this.size = size;
			this.scale = scale;
		}


		/// <summary>
		/// Returns the size of the number (-1 is don't care).
		/// </summary>
		public int Size {
			get { return size; }
		}

		/// <summary>
		/// Returns the scale of the number (-1 is don't care).
		/// </summary>
		public int Scale {
			get { return scale; }
		}

		// ---------- Implemented from TType ----------

		/// <inheritdoc/>
		public override bool IsComparableType(TType type) {
			return (type is TNumericType ||
					type is TBooleanType);
		}

		/// <inheritdoc/>
		public override int Compare(Object ob1, Object ob2) {
			BigNumber n1 = (BigNumber)ob1;
			BigNumber n2;

			if (ob2 is BigNumber) {
				n2 = (BigNumber)ob2;
			} else {
				n2 = (bool)ob2 ? BigNumber.One : BigNumber.Zero;
			}

			return n1.CompareTo(n2);
		}

		/// <inheritdoc/>
		public override int CalculateApproximateMemoryUse(Object ob) {
			// A heuristic - it's difficult to come up with an accurate number
			// for this.
			return 25 + 16;
		}

		/// <inheritdoc/>
		public override Type GetObjectType() {
			return typeof(BigNumber);
		}
	}
}