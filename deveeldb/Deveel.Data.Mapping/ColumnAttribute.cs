//  
//  ColumnAttribute.cs
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

namespace Deveel.Data.Mapping {
	[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = false, Inherited = true)]
	public sealed class ColumnAttribute : Attribute {
		public ColumnAttribute(string name, SqlType type, int size, int scale) {
			this.name = name;
			this.type = type;
			this.size = size;
			this.scale = scale;
		}

		public ColumnAttribute(string name, SqlType type, int size)
			: this(name, type, size, -1) {
		}

		public ColumnAttribute(string name, SqlType type)
			: this(name, type, -1) {
		}

		public ColumnAttribute(string name)
			: this(name, SqlType.Unknown) {
		}

		public ColumnAttribute(SqlType type, int size, int scale)
			: this(null, type, size, scale) {
		}

		public ColumnAttribute(SqlType type, int size)
			: this(type, size, -1) {
		}

		public ColumnAttribute(SqlType type)
			: this(type, -1) {
		}

		public ColumnAttribute(int size, int scale)
			: this(null, SqlType.Unknown, size, scale) {
		}

		public ColumnAttribute(int size)
			: this(size, -1) {
		}

		public ColumnAttribute()
			: this(null, SqlType.Unknown, -1, -1) {
		}

		private string name;
		private SqlType type;
		private int size;
		private int scale;

		public string ColumnName {
			get { return name; }
			set { name = value; }
		}

		public SqlType SqlType {
			get { return type; }
			set { type = value; }
		}

		public int Size {
			get { return size; }
			set { size = value; }
		}

		public int Scale {
			get { return scale; }
			set { scale = value; }
		}
	}
}