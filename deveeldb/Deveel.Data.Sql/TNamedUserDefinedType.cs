//  
//  TNamedUserDefinedType.cs
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

namespace Deveel.Data.Sql {
	/// <summary>
	/// An temporary implementation of <see cref="TType"/>
	/// used to handle parsing named types during a SQL
	/// statement (that defines an UDT by its name).
	/// </summary>
	internal class TNamedUserDefinedType : TType {
		public TNamedUserDefinedType(string typeName) 
			: base(SqlType.Object) {
			this.typeName = typeName;
		}

		private readonly string typeName;

		#region Overrides of TType

		public string TypeName {
			get { return typeName; }
		}

		public override int Compare(object x, object y) {
			throw new InvalidOperationException();
		}

		public override bool IsComparableType(TType type) {
			throw new InvalidOperationException();
		}

		public override int CalculateApproximateMemoryUse(object ob) {
			throw new InvalidOperationException();
		}

		public override Type GetObjectType() {
			return typeof(string);
		}

		#endregion
	}
}