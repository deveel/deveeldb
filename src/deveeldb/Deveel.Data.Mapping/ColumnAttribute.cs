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