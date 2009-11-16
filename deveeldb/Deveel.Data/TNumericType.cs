//  
//  TNumericType.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
// 
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

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