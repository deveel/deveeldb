//  
//  TNullType.cs
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

namespace Deveel.Data {
	///<summary>
	/// An implementation of <see cref="TType"/> that represents 
	/// a <c>NULL</c> type.
	///</summary>
	/// <remarks>
	/// A Null type is an object that can't be anything else except null.
	/// </remarks>
	[Serializable]
	public class TNullType : TType {
		public TNullType()
			// There is no SQL type for a query plan node so we make one up here
			: base(SQLTypes.NULL) {
		}

		/// <inheritdoc/>
		public override bool IsComparableType(TType type) {
			return (type is TNullType);
		}

		/// <inheritdoc/>
		public override int Compare(Object ob1, Object ob2) {
			// It's illegal to compare NULL types with this method so we throw an
			// exception here (see method specification).
			throw new ApplicationException("Compare can not compare NULL types.");
		}

		/// <inheritdoc/>
		public override int CalculateApproximateMemoryUse(Object ob) {
			return 16;
		}

		/// <inheritdoc/>
		public override Type GetObjectType() {
			return typeof(Object);
		}
	}
}