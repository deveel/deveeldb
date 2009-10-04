//  
//  ColumnDescription.cs
//  
//  Author:
//       Antonello Provenzano <antonello@deveel.com>
//       Tobias Downer <toby@mckoi.com>
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
using System.IO;

namespace Deveel.Data {
	/// <summary>
	/// This is a description of a column and the data it stores.
	/// </summary>
	/// <remarks>
	/// Specifically it stores the 'type' as defined input the <see cref="DbTypes"/> 
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
		/// The type of the field, from the <see cref="DbTypes"/> object.
		/// </summary>
		private readonly DbTypes type;

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
		private SQLTypes sql_type = SQLTypes.UNKNOWN;

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
		public ColumnDescription(String name, DbTypes type, int size, bool not_null) {
			this.name = name;
			this.type = type;
			this.size = size;
			this.not_null = not_null;
			unique = false;
			unique_group = -1;
		}

		public ColumnDescription(String name, DbTypes type, bool not_null)
			: this(name, type, -1, not_null) {
		}

		public ColumnDescription(ColumnDescription cd)
			: this(cd.Name, cd.Type, cd.Size, cd.IsNotNull) {
			if (cd.IsUnique) {
				SetUnique();
			}
			UniqueGroup = cd.UniqueGroup;
			Scale = cd.Scale;
			SQLType = cd.SQLType;
		}

		public ColumnDescription(String name, ColumnDescription cd)
			: this(name, cd.Type, cd.Size, cd.IsNotNull) {
			if (cd.IsUnique) {
				SetUnique();
			}
			UniqueGroup = cd.UniqueGroup;
			Scale = cd.Scale;
			SQLType = cd.SQLType;
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
		/// The types are outlined input <see cref="DbTypes"/>.
		/// </remarks>
		public DbTypes Type {
			get { return type; }
		}

		/// <summary>
		/// Returns true if this column is a numeric type.
		/// </summary>
		public bool IsNumericType {
			get { return (type == DbTypes.DB_NUMERIC); }
		}


		/// <summary>
		/// Gets or sets the SQL type for this field.
		/// </summary>
		/// <remarks>
		/// This is only used to emulate SQL types input the database. They are 
		/// mapped to the simpler internal types as follows:
		/// <code>
		///     DB_STRING := CHAR, VARCHAR, LONGVARCHAR
		///    DB_NUMERIC := TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, REAL,
		///                  DOUBLE, NUMERIC, DECIMAL
		///       DB_DATE := DATE, TIME, TIMESTAMP
		///    DB_BOOLEAN := BIT
		///       DB_BLOB := BINARY, VARBINARY, LONGVARBINARY
		///     DB_OBJECT := OBJECT
		/// </code>
		/// </remarks>
		public SQLTypes SQLType {
			get {
				if (sql_type == SQLTypes.UNKNOWN) {
					// If sql type is unknown find from internal type
					if (type == DbTypes.DB_NUMERIC)
						return SQLTypes.NUMERIC;
					if (type == DbTypes.DB_STRING)
						return SQLTypes.LONGVARCHAR;
					if (type == DbTypes.DB_BOOLEAN)
						return SQLTypes.BIT;
					if (type == DbTypes.DB_TIME)
						return SQLTypes.TIMESTAMP;
					if (type == DbTypes.DB_BLOB)
						return SQLTypes.LONGVARBINARY;
					if (type == DbTypes.DB_OBJECT)
						return SQLTypes.OBJECT;
					throw new ApplicationException("Unrecognised internal type.");
				}
				return sql_type;
			}
			set { sql_type = value; }
		}

		/// <summary>
		/// Returns the object <see cref="System.Type"/> for this field.
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
				if (type == DbTypes.DB_BLOB ||
				    type == DbTypes.DB_OBJECT) {
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
			DbTypes type = (DbTypes) input.ReadInt32();
			int size = input.ReadInt32();
			bool not_null = input.ReadBoolean();
			bool unique = input.ReadBoolean();
			int unique_group = input.ReadInt32();

			ColumnDescription col_desc = new ColumnDescription(name, type, size, not_null);
			if (unique) col_desc.SetUnique();
			col_desc.UniqueGroup = unique_group;
			col_desc.SQLType = (SQLTypes) input.ReadInt32();
			col_desc.Scale = input.ReadInt32();

			return col_desc;
		}
	}
}