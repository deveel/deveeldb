//  
//  UserTypeMemberDef.cs
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
	/// <summary>
	/// The class that defines a single member of a 
	/// user-defined type.
	/// </summary>
	public sealed class UserTypeMemberDef {
		public UserTypeMemberDef(string name, TType type) {
			this.name = name;
			this.type = type;
		}

		/// <summary>
		/// The name of the member.
		/// </summary>
		private readonly string name;
		private bool nullable;

		private int offset;

		/// <summary>
		/// The <see cref="TType"/> instance that represents 
		/// the type of the member.
		/// </summary>
		private readonly TType type;

		public TType Type {
			get { return type; }
		}

		/// <summary>
		/// Gets the offset of the member within the parent
		/// user-defined type.
		/// </summary>
		public int Offset {
			get { return offset; }
		}

		public int Size {
			get {
				if (type is TNumericType)
					return (type as TNumericType).Size;
				if (type is TStringType)
					return (type as TStringType).MaximumSize;
				if (type is TIntervalType)
					return (type as TIntervalType).Length;
				if (type is TBinaryType)
					return (type as TBinaryType).MaximumSize;
				return -1;
			}
		}

		public int Scale {
			get {
				TNumericType numericType = type as TNumericType;
				return numericType == null ? -1 : numericType.Scale;
			}
		}

		public SqlType SqlType {
			get { return type.SQLType; }
		}

		/// <summary>
		/// Gets the name of the member.
		/// </summary>
		public string Name {
			get { return name; }
		}

		/// <summary>
		/// Gets or sets a boolean value indicating if the
		/// member can contains <c>NULL</c> values.
		/// </summary>
		public bool Nullable {
			get { return nullable; }
			set { nullable = value; }
		}

		internal void SetOffset(int value) {
			offset = value;
		}
	}
}