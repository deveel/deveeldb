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
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Transactions;
using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	/// <summary>
	/// Defines the metadata properties of a table existing 
	/// within a database.
	/// </summary>
	/// <remarks>
	/// A table structure implements a unique name within a
	/// database system, and a list columns that shape the
	/// design of the data that the table can accommodate.
	/// </remarks>
	public sealed class TableInfo : IObjectInfo, IEnumerable<ColumnInfo> {
		private readonly IList<ColumnInfo> columns;
		private readonly Dictionary<ObjectName, int> columnsCache;

		/// <summary>
		/// Constructs the object with the given table name.
		/// </summary>
		/// <param name="tableName">The unique name of the table within
		/// the database system.</param>
		/// <param name="id">The unique identifier of the table in the database.</param>
		/// <exception cref="ArgumentNullException">
		/// If the provided <paramref name="tableName"/> is <c>null</c>.
		/// </exception>
		public TableInfo(ObjectName tableName)
			: this(tableName, -1, false, new List<ColumnInfo>(), false) {
		}

		private TableInfo(ObjectName tableName, int id, bool perm, IList<ColumnInfo> columns, bool isReadOnly) {
			if (tableName == null) 
				throw new ArgumentNullException("tableName");

			TableName = tableName;
			Id = id;
			IsPermanent = perm;
			this.columns = columns;
			IsReadOnly = isReadOnly;

			columnsCache = new Dictionary<ObjectName, int>();
		}

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.Table; }
		}

		/// <summary>
		/// Gets the fully qualified name of the table that is ensured 
		/// to be unique within the system.
		/// </summary>
		public ObjectName TableName { get; private set; }

		ObjectName IObjectInfo.FullName {
			get { return TableName; }
		}

		/// <summary>
		/// Gets a unique identifier of the table in a database system.
		/// </summary>
		/// <seealso cref="IsPermanent"/>
		public int Id { get; private set; }

		/// <summary>
		/// Gets a value that indicates if the table is permanent.
		/// </summary>
		/// <seealso cref="ITransaction.CreateTable"/>
		public bool IsPermanent { get; private set; }

		/// <summary>
		/// Gets the name part of the table name.
		/// </summary>
		/// <seealso cref="TableName"/>
		/// <seealso cref="ObjectName.Name"/>
		public string Name {
			get { return TableName.Name; }
		}

		/// <summary>
		/// Gets the schema name part of the table name.
		/// </summary>
		/// <seealso cref="TableName"/>
		/// <seealso cref="ObjectName.Parent"/>
		public ObjectName SchemaName {
			get { return TableName.Parent; }
		}

		/// <summary>
		/// Gets the name of the catalog containing the table, if defined.
		/// </summary>
		public string CatalogName {
			get { return SchemaName != null && SchemaName.Parent != null ? SchemaName.Parent.Name : null; }
		}

		///// <summary>
		///// Gets or sets a boolean value that indicates if the column names
		///// will be resolved in case-insensitive mode.
		///// </summary>
		///// <remarks>
		///// By default the value of this flag is set to <c>true</c>.
		///// </remarks>
		//public bool IgnoreCase { get; set; }

		/// <summary>
		/// Gets a boolean value that indicates if the structure of this
		/// table cannot be altered.
		/// </summary>
		public bool IsReadOnly { get; private set; }

		/// <summary>
		/// Gets a count of the <see cref="ColumnInfo">columns</see> 
		/// defined by this object.
		/// </summary>
		public int ColumnCount {
			get { return columns.Count; }
		}

		/// <summary>
		/// Gets the column object defined at the given offset within 
		/// the table.
		/// </summary>
		/// <param name="offset">The zero-based offset of the column to return.</param>
		/// <returns>
		/// Returns an object of type <see cref="ColumnInfo"/> that is
		/// at the given offset within the table.
		/// </returns>
		/// <exception cref="ArgumentOutOfRangeException">
		/// If the given <paramref name="offset"/> is less than zero or
		/// greater or equal than the number of columns defined in the table.
		/// </exception>
		public ColumnInfo this[int offset] {
			get {
				if (offset < 0 || offset >= columns.Count)
					throw new ArgumentOutOfRangeException("offset");

				return columns[offset];
			}
		}

		private void AssertNotReadOnly() {
			if (IsReadOnly)
				throw new InvalidOperationException();
		}

		public void Establish(int id) {
			Id = id;
			IsPermanent = true;
		}

		internal void AddColumnSafe(ColumnInfo column) {
			if (column == null)
				throw new ArgumentNullException("column");

			AssertNotReadOnly();

			columnsCache.Clear();
			column.TableInfo = this;
			columns.Add(column);
		}

		/// <summary>
		/// Adds a new column to the table at the last position of the
		/// columns list in the table metadata.
		/// </summary>
		/// <param name="column">The <see cref="ColumnInfo"/> metadata to
		/// add to the table.</param>
		/// <exception cref="ArgumentNullException">
		/// If the given <paramref name="column"/> is <c>null</c>.
		/// </exception>
		/// <exception cref="InvalidOperationException">
		/// If the table is immutable (<see cref="IsReadOnly"/> is equals to <c>true</c>)
		/// </exception>
		/// <exception cref="ArgumentException">
		/// If the column is already defined in this table or if it
		/// is attacted to another table.
		/// </exception>
		public void AddColumn(ColumnInfo column) {
			if (column == null)
				throw new ArgumentNullException("column");

			AssertNotReadOnly();

			if (column.TableInfo != null &&
			    column.TableInfo != this)
				throw new ArgumentException(String.Format("The column {0} belongs to another table already ({1})", column.ColumnName,
					column.TableInfo.Name));

			if (columns.Any(x => x.ColumnName == column.ColumnName))
				throw new ArgumentException(String.Format("Column {0} is already defined in table {1}", column.ColumnName, TableName));

			columnsCache.Clear();
			column.TableInfo = this;
			columns.Add(column);
		}

		/// <summary>
		/// Adds a new column to the table having the given name and type.
		/// </summary>
		/// <param name="columnName">The name of the column to add.</param>
		/// <param name="columnType">The <see cref="DataType"/> of the column to add.</param>
		/// <returns>
		/// Returns an instance of <see cref="ColumnInfo"/> that is generated by
		/// the given parameters.
		/// </returns>
		/// <seealso cref="AddColumn(string, DataType, bool)"/>
		public ColumnInfo AddColumn(string columnName, DataType columnType) {
			return AddColumn(columnName, columnType, false);
		}

		/// <summary>
		/// Adds a new column to the table having the given name and type.
		/// </summary>
		/// <param name="columnName">The name of the column to add.</param>
		/// <param name="columnType">The <see cref="DataType"/> of the column to add.</param>
		/// <param name="notNull">If the column values must be <c>NOT NULL</c>.</param>
		/// <returns>
		/// Returns an instance of <see cref="ColumnInfo"/> that is generated by
		/// the given parameters.
		/// </returns>
		/// <seealso cref="AddColumn(string, DataType, bool)"/>
		/// <exception cref="ArgumentNullException">
		/// If either <paramref name="columnName"/> or the <paramref name="columnType"/>
		/// arguments are <c>null</c>.
		/// </exception>
		/// <seealso cref="AddColumn(ColumnInfo)"/>
		public ColumnInfo AddColumn(string columnName, DataType columnType, bool notNull) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");

			if (columnType == null)
				throw new ArgumentNullException("columnType");

			var column = new ColumnInfo(columnName, columnType);
			column.IsNotNull = notNull;
			AddColumn(column);
			return column;
		}

		/// <inheritdoc/>
		public IEnumerator<ColumnInfo> GetEnumerator() {
			return columns.GetEnumerator();
		}

		IEnumerator IEnumerable.GetEnumerator() {
			return GetEnumerator();
		}

		/// <summary>
		/// Gets the offset of the column with the given name.
		/// </summary>
		/// <param name="columnName">The name of the column of which
		/// to get the offset.</param>
		/// <returns>
		/// Returns a zero-based index of a column with the given name,
		/// if defined by the table metadata, or -1 otherwise.
		/// </returns>
		public int IndexOfColumn(string columnName) {
			return IndexOfColumn(new ObjectName(TableName, columnName));
		}

		public int IndexOfColumn(ObjectName columnName) {
			int index;
			if (!columnsCache.TryGetValue(columnName, out index)) {
				bool found = false;
				for (int i = 0; i < columns.Count; i++) {
					var column = columns[i];
					if (column.ColumnName.Equals(columnName.Name, StringComparison.Ordinal)) {
						index = i;
						columnsCache[columnName] = index;
						found = true;
					}
				}

				if (!found)
					index = -1;
			}

			return index;
		}

		/// <summary>
		/// Creates a new instance of <see cref="TableInfo"/> as an immutable copy
		/// of this table metadata.
		/// </summary>
		/// <returns>
		/// Returns a new read-only instance of <see cref="TableInfo"/> that is
		/// a copy of this table metadata.
		/// </returns>
		public TableInfo AsReadOnly() {
			return new TableInfo(TableName, Id, IsPermanent, new ReadOnlyCollection<ColumnInfo>(columns), true);
		}

		public TableInfo Alias(ObjectName alias) {
			return new TableInfo(alias, Id, IsPermanent, new ReadOnlyCollection<ColumnInfo>(columns), true);
		}

		internal SqlExpression ResolveColumns(bool ignoreCase, SqlExpression expression) {
			throw new NotImplementedException();
		}

		public IEnumerable<int> IndexOfColumns(IEnumerable<string> columnNames) {
			if (columnNames == null)
				return new int[0];

			return columnNames.Select(IndexOfColumn).ToArray();
		}

		public static void SerializeTo(TableInfo tableInfo, Stream stream) {
			var writer = new BinaryWriter(stream, Encoding.Unicode);
			writer.Write(3);	// Version

			var catName = tableInfo.CatalogName;
			if (catName == null)
				catName = String.Empty;

			writer.Write(catName);
			writer.Write(tableInfo.SchemaName.Name);
			writer.Write(tableInfo.Name);

			var colCount = tableInfo.columns.Count;
			writer.Write(colCount);
			for (int i = 0; i < colCount; i++) {
				var column = tableInfo.columns[i];
				ColumnInfo.SerializeTo(column, writer);
			}
		}

		internal static TableInfo DeserializeFrom(Stream stream, ITypeResolver typeResolver) {
			var reader = new BinaryReader(stream, Encoding.Unicode);

			var version = reader.ReadInt32();
			if (version != 3)
				throw new FormatException("Invalid version of the table info.");

			var catName = reader.ReadString();
			var schemName = reader.ReadString();
			var tableName = reader.ReadString();

			var objSchemaName = !String.IsNullOrEmpty(catName)
				? new ObjectName(new ObjectName(catName), schemName)
				: new ObjectName(schemName);

			var objTableName = new ObjectName(objSchemaName, tableName);

			var tableInfo = new TableInfo(objTableName);

			var colCount = reader.ReadInt32();
			for (int i = 0; i < colCount; i++) {
				var columnInfo = ColumnInfo.DeserializeFrom(stream, typeResolver);

				if (columnInfo != null)
					tableInfo.AddColumn(columnInfo);
			}

			return tableInfo;
		}
	}
}