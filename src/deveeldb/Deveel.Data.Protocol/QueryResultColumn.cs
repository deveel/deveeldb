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
	public class QueryResultColumn {
		/// <summary>
		/// The size of the type.  The meaning of this field changes depending on the
		/// type.  For example, the size of an SQL NUMERIC represents the number of
		/// digits input the value (precision).
		/// </summary>
		private readonly int size;

		/// <summary>
		/// The SQL standard type as defined input. This is required to emulate
		/// the various SQL types. The value is initialised to -9332 to indicate
		/// the sql type has not be defined.
		/// </summary>
		private SqlType sqlType = SqlType.Unknown;

		/// <summary>
		/// The Constructors if the type does require a size.
		/// </summary>
		/// <param name="name"></param>
		/// <param name="type"></param>
		/// <param name="size"></param>
		/// <param name="notNull"></param>
		private QueryResultColumn(String name, DbType type, int size, bool notNull) {
			Scale = -1;
			this.Name = name;
			this.Type = type;
			this.size = size;
			this.IsNotNull = notNull;
			IsUnique = false;
			UniqueGroup = -1;
		}

		internal QueryResultColumn(string name, DataColumnInfo columnInfo)
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

		/// <summary>
		/// Returns an integer representing the type of the field.
		/// </summary>
		/// <remarks>
		/// The types are outlined input <see cref="DbType"/>.
		/// </remarks>
		public DbType Type { get; private set; }

		/// <summary>
		/// Returns true if this column is a numeric type.
		/// </summary>
		public bool IsNumericType {
			get { return (Type == DbType.Numeric); }
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
		public SqlType SqlType {
			get {
				if (sqlType == SqlType.Unknown) {
					// If sql type is unknown find from internal type
					if (Type == DbType.Numeric)
						return SqlType.Numeric;
					if (Type == DbType.String)
						return SqlType.VarChar;
					if (Type == DbType.Boolean)
						return SqlType.Bit;
					if (Type == DbType.Time)
						return SqlType.TimeStamp;
					if (Type == DbType.Blob)
						return SqlType.LongVarBinary;
					if (Type == DbType.Object)
						return SqlType.Object;
					throw new ApplicationException("Unrecognised internal type.");
				}
				return sqlType;
			}
			set { sqlType = value; }
		}

		/// <summary>
		/// Returns the object <see cref="System.Type"/> for this field.
		/// </summary>
		public Type ObjectType {
			get { return TypeUtil.ToType(Type); }
		}

		public Type RuntimeType {
			get { return TypeUtil.ToRuntimeType(SqlType); }
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
		public int Scale { get; set; }

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
				if (Type == DbType.Blob ||
				    Type == DbType.Object) {
					return false;
				}
				return true;
			}
		}

		public bool IsAliased {
			get { return Name.StartsWith("@a"); }
		}

		/// <inheritdoc/>
		public override bool Equals(Object ob) {
			var cd = (QueryResultColumn)ob;
			return (Name.Equals(cd.Name) &&
					Type == cd.Type &&
					size == cd.size &&
					IsNotNull == cd.IsNotNull &&
					IsUnique == cd.IsUnique &&
					UniqueGroup == cd.UniqueGroup);
		}

		/// <inheritdoc/>
		public override int GetHashCode() {
			return base.GetHashCode();
		}

		/// <summary>
		/// Writes this <see cref="QueryResultColumn"/> to the given <see cref="BinaryWriter"/>.
		/// </summary>
		/// <param name="output"></param>
		public void WriteTo(BinaryWriter output) {
			output.Write(Name);
			output.Write((int)Type);
			output.Write(size);
			output.Write(IsNotNull);
			output.Write(IsUnique);
			output.Write(UniqueGroup);
			output.Write((int)sqlType);
			output.Write(Scale);
		}


		/// <summary>
		/// Reads a <see cref="QueryResultColumn"/> from the given 
		/// <see cref="BinaryReader"/> and returns a new instance of it.
		/// </summary>
		/// <param name="input"></param>
		/// <returns></returns>
		public static QueryResultColumn ReadFrom(BinaryReader input) {
			String name = input.ReadString();
			DbType type = (DbType) input.ReadInt32();
			int size = input.ReadInt32();
			bool notNull = input.ReadBoolean();
			bool unique = input.ReadBoolean();
			int uniqueGroup = input.ReadInt32();

			var colDesc = new QueryResultColumn(name, type, size, notNull);
			if (unique) colDesc.SetUnique();
			colDesc.UniqueGroup = uniqueGroup;
			colDesc.SqlType = (SqlType) input.ReadInt32();
			colDesc.Scale = input.ReadInt32();

			return colDesc;
		}
	}
}