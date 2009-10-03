// 
//  TArrayType.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
//  
//  Copyright (c) 2009 Deveel
// 
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU Lesser General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU Lesser General Public License for more details.
// 
//  You should have received a copy of the GNU Lesser General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.

using System;

namespace Deveel.Data {
	/// <summary>
	/// An implementation of TType for an expression array.
	/// </summary>
	[Serializable]
	public class TArrayType : TType {
		///<summary>
		/// Constructs a new <see cref="TArrayType"/>.
		///</summary>
		public TArrayType()
			// There is no SQL type for a query plan node so we make one up here
			: base(SQLTypes.ARRAY) {
		}

		/// <inheritdoc/>
		/// <exception cref="NotSupportedException"/>
		public override bool IsComparableType(TType type) {
			throw new NotSupportedException("Query Plan types should not be compared.");
		}

		/// <inheritdoc/>
		/// <exception cref="NotSupportedException"/>
		public override int Compare(Object ob1, Object ob2) {
			throw new NotSupportedException("Query Plan types should not be compared.");
		}

		/// <inheritdoc/>
		public override int CalculateApproximateMemoryUse(Object ob) {
			return 5000;
		}

		/// <inheritdoc/>
		public override Type GetObjectType() {
			return typeof(Expression[]);
		}

	}
}