// 
//  Copyright 2010-2014 Deveel
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
using System.IO;

using Deveel.Data.Sql;
using Deveel.Data.Types;

namespace Deveel.Data.Protocol {
	public class QueryResultColumn {
		/// <summary>
		/// The Constructors if the type does require a size.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="type"></param>
		/// <param name="notNull"></param>
		private QueryResultColumn(string name, DataType type, bool notNull) {
			Name = name;
			Type = type;
			IsNotNull = notNull;
			IsUnique = false;
			UniqueGroup = -1;
		}

		internal QueryResultColumn(string name, ColumnInfo columnInfo)
			: this(name, columnInfo.ColumnType, columnInfo.IsNotNull) {

		}

		/// <summary>
		/// Sets this column to unique.
		/// </summary>
		/// <remarks>
		/// <b>Note</b>: This can only happen during the setup of 
		/// the object. Unpredictable results will occur otherwise.
		/// </remarks>
		public void SetUnique() {
			IsUnique = true;
		}

		/// <summary>
		/// Returns the name of the field.
		/// </summary>
		/// <remarks>
		/// The field type returned should be <i>ZIP</i> or <i>Address1</i>. 
		/// To resolve to the tables type, we must append an additional 
		/// <i>Company.</i> or <i>Customer.</i> string to the front.
		/// </remarks>
		public string Name { get; private set; }

		public DataType Type { get; private set; }

		/// <summary>
		/// Returns true if this column is a numeric type.
		/// </summary>
		public bool IsNumericType {
			get { return (Type is NumericType); }
		}


		/// <summary>
		/// Returns the size of the given field.  This is only applicable to a 
		/// few of the types, ie VARCHAR.
		/// </summary>
		public int Size {
			get { return (Type is ISizeableType) ? ((ISizeableType)Type).Size : -1; }
		}

		/// <summary>
		/// If this is a number, gets or sets the scale of the field.
		/// </summary>
		/// <returns></returns>
		public int Scale {
			get { return (Type is NumericType) ? ((NumericType) Type).Scale : -1; }
		}

		/// <summary>
		/// Determines whether the field can contain a null value or not.
		/// </summary>
		/// <value>
		/// Returns true if it is required for the column to contain data.
		/// </value>
		public bool IsNotNull { get; private set; }

		/// <summary>
		/// Determines whether the field can contain two items that are identical.
		/// </summary>
		/// <remarks>
		/// Returns true if each element must be unique.
		/// </remarks>
		public bool IsUnique { get; private set; }

		/// <summary>
		/// Gets or sets the unique group this column is input or -1 if it does
		/// not belong to a unique group.
		/// </summary>
		/// <remarks>
		/// <b>Note</b>: This can only happen during the setup of the object. 
		/// Unpredictable results will occur otherwise.
		/// </remarks>
		public int UniqueGroup { get; set; }

		public bool IsAliased {
			get { return Name.StartsWith("@a"); }
		}

		public Type RuntimeType {
			get { return Type.GetRuntimeType(); }
		}

		public Type ValueType {
			get { return Type.GetObjectType(); }
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			var cd = (QueryResultColumn)ob;
			return (Name.Equals(cd.Name) &&
					Type == cd.Type &&
					Size == cd.Size &&
					IsNotNull == cd.IsNotNull &&
					IsUnique == cd.IsUnique &&
					UniqueGroup == cd.UniqueGroup);
		}

		public override int GetHashCode() {
			return base.GetHashCode();
		}
	}
}