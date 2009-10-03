// 
//  TObjectType.cs
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
	/// An implementation of TType for a object of possibly defined type.
	/// </summary>
	[Serializable]
	public class TObjectType : TType {
		/// <summary>
		/// The type of class this is contrained to or null if it is not constrained to a <see cref="Type"/>.
		/// </summary>
		private readonly String type_name;

		public TObjectType(String type_name)
			: base(SQLTypes.OBJECT) {
			this.type_name = type_name;
		}

		public TObjectType(Type type)
			: this(type.FullName) {
		}

		/// <summary>
		/// Gets the string describing the <see cref="Type"/>.
		/// </summary>
		public string TypeString {
			get { return type_name; }
		}

		/// <inheritdoc/>
		public override bool IsComparableType(TType type) {
			return (type is TObjectType);
		}

		/// <inheritdoc/>
		public override int Compare(Object ob1, Object ob2) {
			throw new ApplicationException("Object types can not be compared.");
		}

		/// <inheritdoc/>
		public override int CalculateApproximateMemoryUse(Object ob) {
			if (ob != null) {
				return ((ByteLongObject)ob).Length + 4;
			} else {
				return 4 + 8;
			}
		}

		/// <inheritdoc/>
		public override Type GetObjectType() {
			return typeof(ByteLongObject);
		}
	}
}