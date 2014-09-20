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

using Deveel.Data.DbSystem;
using Deveel.Data.Sql;

namespace Deveel.Data.Protocol {
	/// <summary>
	/// This is a description of a column and the data it stores.
	/// </summary>
	/// <remarks>
	/// Specifically it stores the 'type' as defined input the <see cref="DbType"/> 
	/// class, the 'size' if the column cells may be different lengths 
	/// (eg, string), the name of the column, whether the column set must 
	/// contain unique elements, and whether a cell may be added that is null.
	/// </remarks>
	[Serializable]
	public class ColumnDescription {
		/// <summary>
		/// The name of the field.
		/// </summary>
		private readonly String name;

		/// <summary>
		/// The type of the field, from the <see cref="DbType"/> object.
		/// </summary>
		private readonly DbType type;

		/// <summary>
		/// The size of the type.  The meaning of this field changes depending on the
		/// type.  For example, the size of an SQL NUMERIC represents the number of
		/// digits input the value (precision).
		/// </summary>
		private readonly int size;

		/// <summary>
		/// The scale of a numerical value.  This represents the number of digits to
		/// the right of the decimal point.  The number is rounded to this scale
		/// input arithmatic operations.  By default, the scale is '10'
		/// </summary>
		private int scale = -1;

		/// <summary>
		/// The SQL standard type as defined input. This is required to emulate
		/// the various SQL types. The value is initialised to -9332 to indicate
		/// the sql type has not be defined.
		/// </summary>
		private SqlType sql_type = SqlType.Unknown;

		/// <summary>
		/// If true, the field may not be null.  If false, the column may contain
		/// no information.  This is enforced at the parse stage when adding or
		/// altering a table.
		/// </summary>
		private readonly bool not_null;

		/// <summary>
		/// If true, the field may only contain unique values.  This is enforced at
		/// the parse stage when adding or altering a table.
		/// </summary>
		private bool unique;

		/// <summary>
		/// This represents the 'unique_group' that this column is input.  If two
		/// columns input a table belong to the same unique_group, then the specific
		/// combination of the groups columns can not exist more than once input the
		/// table.
		/// A value of -1 means the column does not belong to any unique group.
		/// </summary>
		private int unique_group;

		/// <summary>
		/// The Constructors if the type does require a size.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="type"></param>
		/// <param name="size"></param>
		/// <param name="not_null"></param>
		private ColumnDescription(String name, DbType type, int size, bool not_null) {
			this.name = name;
			this.type = type;
			this.size = size;
			this.not_null = not_null;
			unique = false;
			unique_group = -1;
		}

		internal ColumnDescription(string name, DataColumnInfo columnInfo)
			: this(name, columnInfo.TType.DbType, columnInfo.Size, columnInfo.IsNotNull) {

		}

		/// <summary>
		/// Sets this column to unique.
		/// </summary>
		/// <remarks>
		/// <b>Note</b>: This can only happen during the setup of 
		/// the object. Unpredictable results will occur otherwise.
		/// </remarks>
		public void SetUnique() {
			unique = true;
		}

		/// <summary>
		/// Returns the name of the field.
		/// </summary>
		/// <remarks>
		/// The field type returned should be <i>ZIP</i> or <i>Address1</i>. 
		/// To resolve to the tables type, we must append an additional 
		/// <i>Company.</i> or <i>Customer.</i> string to the front.
		/// </remarks>
		public string Name {
			get { return name; }
		}

		/// <summary>
		/// Returns an integer representing the type of the field.
		/// </summary>
		/// <remarks>
		/// The types are outlined input <see cref="DbType"/>.
		/// </remarks>
		public DbType Type {
			get { return type; }
		}

		/// <summary>
		/// Returns true if this column is a numeric type.
		/// </summary>
		public bool IsNumericType {
			get { return (type == DbType.Numeric); }
		}


		/// <summary>
		/// Gets or sets the SQL type for this field.
		/// </summary>
		/// <remarks>
		/// This is only used to emulate SQL types input the database. They are 
		/// mapped to the simpler internal types as follows:
		/// <code>
		///     STRING := CHAR, VARCHAR, LONGVARCHAR
		///    NUMERIC := TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, REAL,
		///               DOUBLE, NUMERIC, DECIMAL
		///       DATE := DATE, TIME, TIMESTAMP
		///    BOOLEAN := BIT
		///       BLOB := BINARY, VARBINARY, LONGVARBINARY
		///     OBJECT := OBJECT
		/// </code>
		/// </remarks>
		public SqlType SQLType {
			get {
				if (sql_type == SqlType.Unknown) {
					// If sql type is unknown find from internal type
					if (type == DbType.Numeric)
						return SqlType.Numeric;
					if (type == DbType.String)
						return SqlType.LongVarChar;
					if (type == DbType.Boolean)
						return SqlType.Bit;
					if (type == DbType.Time)
						return SqlType.TimeStamp;
					if (type == DbType.Blob)
						return SqlType.LongVarBinary;
					if (type == DbType.Object)
						return SqlType.Object;
					throw new ApplicationException("Unrecognised internal type.");
				}
				return sql_type;
			}
			set { sql_type = value; }
		}

		/// <summary>
		/// Returns the object <see cref="Deveel.Data.DbSystem.Type"/> for this field.
		/// </summary>
		public Type ObjectType {
			get { return TypeUtil.ToType(type); }
		}

		/// <summary>
		/// Returns the size of the given field.  This is only applicable to a 
		/// few of the types, ie VARCHAR.
		/// </summary>
		public int Size {
			get { return size; }
		}

		/// <summary>
		/// If this is a number, gets or sets the scale of the field.
		/// </summary>
		/// <returns></returns>
		public int Scale {
			get { return scale; }
			set { scale = value; }
		}

		/// <summary>
		/// Determines whether the field can contain a null value or not.
		/// </summary>
		/// <value>
		/// Returns true if it is required for the column to contain data.
		/// </value>
		public bool IsNotNull {
			get { return not_null; }
		}

		/// <summary>
		/// Determines whether the field can contain two items that are identical.
		/// </summary>
		/// <remarks>
		/// Returns true if each element must be unique.
		/// </remarks>
		public bool IsUnique {
			get { return unique; }
		}

		/// <summary>
		/// Gets or sets the unique group this column is input or -1 if it does
		/// not belong to a unique group.
		/// </summary>
		/// <remarks>
		/// <b>Note</b>: This can only happen during the setup of the object. 
		/// Unpredictable results will occur otherwise.
		/// </remarks>
		public int UniqueGroup {
			get { return unique_group; }
			set { unique_group = value; }
		}

		/// <summary>
		/// Returns true if the type of the field is searchable.
		/// </summary>
		/// <remarks>
		/// Searchable means that the database driver can quantify it, 
		/// as input determine if a given object of the same type is 
		/// greater, equal or less.  We can not quantify BLOB types.
		/// </remarks>
		public bool IsQuantifiable {
			get {
				if (type == DbType.Blob ||
				    type == DbType.Object) {
					return false;
				}
				return true;
			}
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			ColumnDescription cd = (ColumnDescription)ob;
			return (name.Equals(cd.name) &&
					type == cd.type &&
					size == cd.size &&
					not_null == cd.not_null &&
					unique == cd.unique &&
					unique_group == cd.unique_group);
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return base.GetHashCode();
		}

		/// <summary>
		/// Writes this <see cref="ColumnDescription"/> to the given <see cref="BinaryWriter"/>.
		/// </summary>
		/// <param name="output"></param>
		public void WriteTo(BinaryWriter output) {
			output.Write(name);
			output.Write((int)type);
			output.Write(size);
			output.Write(not_null);
			output.Write(unique);
			output.Write(unique_group);
			output.Write((int)sql_type);
			output.Write(scale);
		}


		/// <summary>
		/// Reads a <see cref="ColumnDescription"/> from the given 
		/// <see cref="BinaryReader"/> and returns a new instance of it.
		/// </summary>
		/// <param name="input"></param>
		/// <returns></returns>
		public static ColumnDescription ReadFrom(BinaryReader input) {
			String name = input.ReadString();
			DbType type = (DbType) input.ReadInt32();
			int size = input.ReadInt32();
			bool not_null = input.ReadBoolean();
			bool unique = input.ReadBoolean();
			int unique_group = input.ReadInt32();

			ColumnDescription col_desc = new ColumnDescription(name, type, size, not_null);
			if (unique) col_desc.SetUnique();
			col_desc.UniqueGroup = unique_group;
			col_desc.SQLType = (SqlType) input.ReadInt32();
			col_desc.Scale = input.ReadInt32();

			return col_desc;
		}
	}
}