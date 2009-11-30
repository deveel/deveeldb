//  
//  TUserDefinedType.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
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
	/// <exclude/>
	public sealed class TUserDefinedType : TType {
		public TUserDefinedType() 
			: base(SqlType.Object) {
		}

		#region Overrides of TType

		public override int Compare(object x, object y) {
			if (x == null && y == null)
				return 0;
			if (x == null)
				return 1;

			if (!(x is IComparable))
				throw new ArgumentException("The first argument is not comparable.");

			IComparable c = (IComparable) x;
			return c.CompareTo(y);
		}

		public override bool IsComparableType(TType type) {
			return type is TUserDefinedType;
		}

		public override int CalculateApproximateMemoryUse(object ob) {
			// delegate to ObjectTransfer to be sure...
			return ObjectTransfer.SizeOf(ob);
		}

		public override Type GetObjectType() {
			return typeof(IUserDefinedType);
		}

		#endregion
	}
}