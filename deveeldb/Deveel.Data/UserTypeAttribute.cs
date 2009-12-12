//  
//  UserTypeAttribute.cs
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
	public sealed class UserTypeAttribute {
		internal UserTypeAttribute(UserType userType, string name, TType type) {
			if (userType == null)
				throw new ArgumentNullException("userType");
			 
			this.userType = userType;
			this.name = name;
			this.type = type;
		}

		// special constructor for SQL statements
		internal UserTypeAttribute(string name, TType type) {
			this.name = name;
			this.type = type;
		}

		/// <summary>
		/// The UDT declaring this member.
		/// </summary>
		internal UserType userType;

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

		/// <summary>
		/// Gets the instance of <see cref="UserType"/> that declares 
		/// the member.
		/// </summary>
		public UserType DeclaringType {
			get { return userType; }
		}

		/// <summary>
		/// Gets the instance of <see cref="TType"/> that defines
		/// the type of data handled by the member.
		/// </summary>
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

		/// <summary>
		/// Gets the size defined by the <see cref="Type"/>, if supported.
		/// </summary>
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

		/// <summary>
		/// Gets the numeric scale defined by the <see cref="Type"/>,
		/// if supported.
		/// </summary>
		public int Scale {
			get {
				TNumericType numericType = type as TNumericType;
				return numericType == null ? -1 : numericType.Scale;
			}
		}

		/// <summary>
		/// Gets the <see cref="Data.SqlType"/> handled.
		/// </summary>
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