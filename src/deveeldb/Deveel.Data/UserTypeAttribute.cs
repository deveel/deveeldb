// 
//  Copyright 2010  Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

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