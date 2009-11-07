//  
//  TBooleanType.cs
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
	/// <summary>
	/// An implementation of TType for a boolean value.
	/// </summary>
	[Serializable]
	public sealed class TBooleanType : TType {
		///<summary>
		///</summary>
		///<param name="sql_type"></param>
		public TBooleanType(SqlType sql_type)
			: base(sql_type) {
		}

		/// <inheritdoc/>
		public override bool IsComparableType(TType type) {
			return (type is TBooleanType ||
					type is TNumericType);
		}

		/// <inheritdoc/>
		public override int Compare(Object ob1, Object ob2) {

			if (ob2 is BigNumber) {
				BigNumber n2 = (BigNumber)ob2;
				BigNumber n1 = !(bool)ob1 ?
								  BigNumber.BIG_NUMBER_ZERO : BigNumber.BIG_NUMBER_ONE;
				return n1.CompareTo(n2);
			}

			if (ob1 == ob2 || ob1.Equals(ob2)) {
				return 0;
			} else if ((bool)ob1) {
				return 1;
			} else {
				return -1;
			}
		}

		/// <inheritdoc/>
		public override int CalculateApproximateMemoryUse(Object ob) {
			return 5;
		}

		/// <inheritdoc/>
		public override Type GetObjectType() {
			return typeof(Boolean);
		}

	}
}