// 
//  Copyright 2010-2015 Deveel
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
//

using System;
using System.IO;
using System.Text;

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql.Tables {
	/// <summary>
	/// Defines the metadata properties of a column within a
	/// table of a database.
	/// </summary>
	/// <remarks>
	/// Columns have unique names within a table and a given
	/// <see cref="SqlType"/> that is used to define the type
	/// of data cells in the table will handle.
	/// </remarks>
	[Serializable]
	public sealed class ColumnInfo : ISerializable {
		/// <summary>
		/// Constructs a new column with the given name and type.
		/// </summary>
		/// <param name="columnName">The name of the column, as case-sensitive and unique 
		/// within the table.</param>
		/// <param name="columnType">The <see cref="SqlType"/> that this column will handle.</param>
		/// <exception cref="ArgumentNullException">
		/// If either one of <paramref name="columnName"/> or <paramref name="columnType"/>
		/// is <c>null</c>.
		/// </exception>
		public ColumnInfo(string columnName, SqlType columnType) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");
			if (columnType == null) 
				throw new ArgumentNullException("columnType");


			ColumnType = columnType;
			ColumnName = columnName;
		}

		private ColumnInfo(ObjectData data) {
			ColumnName = data.GetString("ColumnName");
			ColumnType = data.GetValue<SqlType>("ColumnType");
			IsNotNull = data.GetBoolean("IsNotNull");
			DefaultExpression = data.GetValue<SqlExpression>("Default");
			IndexType = data.GetString("IndexType");
		}

		/// <summary>
		/// Gets the <see cref="TableInfo">table</see> where the column
		/// is attached to.
		/// </summary>
		/// <remarks>
		/// This value is set when this object is added to a table.
		/// </remarks>
		/// <seealso cref="Tables.TableInfo.AddColumn(ColumnInfo)"/>
		public TableInfo TableInfo { get; internal set; }

		/// <summary>
		/// Gets the name of the column.
		/// </summary>
		public string ColumnName { get; private set; }

		/// <summary>
		/// Gets the fully qualified name of the column within the
		/// database system.
		/// </summary>
		/// <remarks>
		/// When <see cref="TableInfo"/> is set, the value returned is the
		/// fully qualified name of the column, otherwise this returns an
		/// instance of <see cref="ObjectName"/> that defines only <see cref="ColumnName"/>.
		/// </remarks>
		public ObjectName FullColumnName {
			get { return TableInfo == null ? new ObjectName(ColumnName) : new ObjectName(TableInfo.TableName, ColumnName); }
		}

		/// <summary>
		/// Gets the <see cref="SqlType"/> that cells within a table for this
		/// column will handle.
		/// </summary>
		/// <seealso cref="SqlType"/>
		public SqlType ColumnType { get; private set; }

		/// <summary>
		/// Gets the zero-based offset of the column within the containing table.
		/// </summary>
		/// <seealso cref="Tables.TableInfo.IndexOfColumn(string)"/>
		public int Offset {
			get { return TableInfo == null ? -1 : TableInfo.IndexOfColumn(ColumnName); }
		}

		public bool HasSize {
			get { return ColumnType is ISizeableType; }
		}

		public int Size {
			get {
				var sizeable = ColumnType as ISizeableType;
				return sizeable == null ? -1 : sizeable.Size;
			}
		}

		public bool HasScale {
			get { return ColumnType is NumericType; }
		}

		public int Scale {
			get {
				var numericType = ColumnType as NumericType;
				return numericType == null ? -1 : numericType.Scale;
			}
		}

		/// <summary>
		/// Gets a boolean vale indicating if the value of a column
		/// can participate to an index.
		/// </summary>
		/// <seealso cref="SqlType.IsIndexable"/>
		public bool IsIndexable {
			get { return ColumnType.IsIndexable; }
		}

		/// <summary>
		/// Gets or sets a boolean value indicating if the column values 
		/// are constrained to be ony <c>NOT NULL</c>.
		/// </summary>
		public bool IsNotNull { get; set; }

		/// <summary>
		/// Gets or sets a <see cref="SqlExpression"/> used as a <c>DEFAULT</c>
		/// when a constraint for the column is to <c>SET DEFAULT</c>.
		/// </summary>
		/// <seealso cref="SqlExpression"/>
		public SqlExpression DefaultExpression { get; set; }

		/// <summary>
		/// Gets a boolean value indicating if the column has a <see cref="DefaultExpression"/>.
		/// </summary>
		/// <seealso cref="DefaultExpression"/>
		public bool HasDefaultExpression {
			get { return DefaultExpression != null; }
		}

		public string IndexType { get; internal set; }


		void ISerializable.GetData(SerializeData data) {
			data.SetValue("ColumnName", ColumnName);
			data.SetValue("ColumnType", ColumnType);
			data.SetValue("IsNotNull", IsNotNull);
			data.SetValue("Default", DefaultExpression);
			data.SetValue("IndexType", IndexType);
		}


		public static void Serialize(ColumnInfo columnInfo, BinaryWriter writer) {
			writer.Write(3);    // Version
			writer.Write(columnInfo.ColumnName);

			TypeSerializer.SerializeTo(writer, columnInfo.ColumnType);

			writer.Write(columnInfo.IsNotNull ? (byte)1 : (byte)0);

			if (columnInfo.DefaultExpression != null) {
				writer.Write((byte)1);
				SqlExpression.Serialize(columnInfo.DefaultExpression, writer);
			} else {
				writer.Write((byte)0);
			}
		}

		public static ColumnInfo Deserialize(BinaryReader reader, ITypeResolver typeResolver) {
			var version = reader.ReadInt32();
			if (version != 3)
				throw new FormatException("Invalid version of the Column-Info");

			var columnName = reader.ReadString();
			var columnType = TypeSerializer.Deserialize(reader, typeResolver);

			var notNull = reader.ReadByte() == 1;

			var columnInfo = new ColumnInfo(columnName, columnType);
			columnInfo.IsNotNull = notNull;

			var hasDefault = reader.ReadByte() == 1;
			if (hasDefault)
				columnInfo.DefaultExpression = SqlExpression.Deserialize(reader);

			return columnInfo;
		}
	}
}