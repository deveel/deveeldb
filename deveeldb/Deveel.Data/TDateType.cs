//  
//  TDateType.cs
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
	/// <summary>
	/// An implementation of TType for date objects.
	/// </summary>
	[Serializable]
	public class TDateType : TType {
		public TDateType(SqlType sql_type)
			: base(sql_type) {
		}

		/// <inheritdoc/>
		public override bool IsComparableType(TType type) {
			return (type is TDateType);
		}

		/// <inheritdoc/>
		public override int Compare(Object ob1, Object ob2) {
			return ((DateTime)ob1).CompareTo((DateTime)ob2);
		}

		/// <inheritdoc/>
		public override int CalculateApproximateMemoryUse(Object ob) {
			return 4 + 8;
		}

		/// <inheritdoc/>
		public override Type GetObjectType() {
			return typeof(DateTime);
		}
	}
}