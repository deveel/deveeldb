// 
//  Copyright 2010-2018 Deveel
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
using System.Linq;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Tables {
	public class TableInfo : IDbObjectInfo, ISqlFormattable {
		public TableInfo(ObjectName tableName) {
			TableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
			Columns = new ColumnCollection(this);
		}

		DbObjectType IDbObjectInfo.ObjectType => DbObjectType.Table;

		ObjectName IDbObjectInfo.FullName => TableName;

		public ObjectName TableName { get; }

		public virtual bool IsReadOnly => false;

		public virtual IColumnList Columns { get; }

		public TableInfo As(ObjectName aliasName)
			=> new AliasedTableInfo(aliasName, this);

		public TableInfo AsReadOnly()
			=> new ReadOnlyTableInfo(this);

		public SqlExpression ResolveColumns(SqlExpression expression, bool ignoreCase) {
			var columnResolver = new ColumnResolver(this, ignoreCase);

			return columnResolver.Visit(expression);
		}

		void ISqlFormattable.AppendTo(SqlStringBuilder builder) {
			TableName.AppendTo(builder);
			builder.AppendLine(" (");
			builder.Indent();

			for (int i = 0; i < Columns.Count; i++) {
				Columns[i].AppendTo(builder);

				if (i < Columns.Count - 1)
					builder.Append(",");

				builder.AppendLine();
			}

			builder.DeIndent();
			builder.Append(")");
		}

		#region ColumnCollection

		class ColumnCollection : IColumnList {
			private readonly TableInfo tableInfo;
			private Dictionary<ObjectName, int> columnsCache;
			private readonly List<ColumnInfo> columns;

			public ColumnCollection(TableInfo tableInfo) : this(tableInfo, new ColumnInfo[0]) {
			}

			public ColumnCollection(TableInfo tableInfo, IEnumerable<ColumnInfo> columns) {
				this.tableInfo = tableInfo;
				this.columns = new List<ColumnInfo>(columns);

				columnsCache = new Dictionary<ObjectName, int>();
			}

			public ColumnInfo this[string columnName] {
				get {
					var offset = IndexOf(columnName);

					if (offset == -1)
						return null;

					return columns[offset];
				}
			}

			private void AssertNotReadOnly() {
				if (IsReadOnly)
					throw new InvalidOperationException($"Table {tableInfo.TableName} is read-only");
			}

			private void AssertColumnInRange(int offset) {
				if (offset < 0 || offset >= columns.Count)
					throw new ArgumentOutOfRangeException(nameof(offset), offset, "The column offset is out of range");
			}

			private void AssertColumnNotExists(ColumnInfo column) {
				if (columns.Any(x => String.Equals(x.ColumnName, column.ColumnName, StringComparison.OrdinalIgnoreCase)))
					throw new ArgumentException($"A column named '{column.ColumnName}' is already present in table '{tableInfo.TableName}'");
			}

			public int IndexOf(string columnName) {
				return IndexOf(new ObjectName(tableInfo.TableName, columnName));
			}

			public int IndexOf(ObjectName columnName) {
				if (columnName.Parent != null &&
				    !tableInfo.TableName.Equals(columnName.Parent, true))
					return -1;

				if (!columnsCache.TryGetValue(columnName, out var offset)) {
					for (int i = 0; i < columns.Count; i++) {
						if (String.Equals(columns[i].ColumnName, columnName.Name, StringComparison.Ordinal)) {
							columnsCache[columnName] = offset = i;

							return offset;
						}
					}
				} else {
					return offset;
				}

				return -1;
			}

			public IEnumerator<ColumnInfo> GetEnumerator() {
				return columns.GetEnumerator();
			}

			IEnumerator IEnumerable.GetEnumerator() {
				return GetEnumerator();
			}

			public void Add(ColumnInfo item) {
				if (item == null) throw new ArgumentNullException(nameof(item));

				AssertNotReadOnly();
				AssertColumnNotExists(item);

				item.TableInfo = tableInfo;
				columns.Add(item);

				columnsCache.Clear();
			}

			public void Clear() {
				AssertNotReadOnly();

				foreach (var column in columns) {
					column.TableInfo = null;
				}

				columns.Clear();
				columnsCache.Clear();
			}

			public bool Contains(ColumnInfo item) {
				if (item == null) throw new ArgumentNullException(nameof(item));

				return IndexOf(item) != -1;
			}

			public void CopyTo(ColumnInfo[] array, int arrayIndex) {
				columns.CopyTo(array, arrayIndex);
			}

			public bool Remove(string columnName) {
				var index = IndexOf(columnName);

				if (index == -1)
					return false;

				RemoveAt(index);

				return true;
			}

			public bool Remove(ColumnInfo item) {
				if (item == null) throw new ArgumentNullException(nameof(item));

				AssertNotReadOnly();

				var index = IndexOf(item);

				if (index == -1)
					return false;

				columns[index].TableInfo = null;
				columns.RemoveAt(index);
				columnsCache.Clear();

				return true;
			}

			public int Count => columns.Count;

			public bool IsReadOnly => tableInfo.IsReadOnly;

			public int IndexOf(ColumnInfo item) {
				if (item == null) throw new ArgumentNullException(nameof(item));

				return IndexOf(item.ColumnName);
			}

			public void Insert(int index, ColumnInfo item) {
				if (item == null) throw new ArgumentNullException(nameof(item));

				AssertNotReadOnly();
				AssertColumnNotExists(item);

				item.TableInfo = tableInfo;
				columns.Insert(index, item);
				columnsCache.Clear();
			}

			public void RemoveAt(int index) {
				AssertNotReadOnly();
				AssertColumnInRange(index);

				columns[index].TableInfo = null;
				columns.RemoveAt(index);
				columnsCache.Clear();
			}

			public ColumnInfo this[int index] {
				get => columns[index];
				set {
					if (value == null) throw new ArgumentNullException(nameof(value));

					AssertNotReadOnly();
					AssertColumnNotExists(value);
					AssertColumnInRange(index);

					value.TableInfo = tableInfo;
					columns[index] = value;

					columnsCache.Clear();
				}
			}

			public ObjectName GetColumnName(int offset) {
				AssertColumnInRange(offset);

				return new ObjectName(tableInfo.TableName, columns[offset].ColumnName);
			}
		}

		#endregion

		#region AliasedTableInfo

		class AliasedTableInfo : TableInfo {
			private readonly ColumnCollection columns;

			public AliasedTableInfo(ObjectName tableName, TableInfo baseTableInfo) : base(tableName) {
				columns = new ColumnCollection(this, Copy(this, baseTableInfo.Columns));
			}

			public override bool IsReadOnly => true;

			public override IColumnList Columns => columns;

			private static IEnumerable<ColumnInfo> Copy(TableInfo tableInfo, IEnumerable<ColumnInfo> columns) {
				return columns.Select(x => 
					new ColumnInfo(x.ColumnName, x.ColumnType, x.DefaultValue){TableInfo = tableInfo});
			}
		}

		#endregion

		#region ReadOnlyTableInfo

		class ReadOnlyTableInfo : TableInfo {
			private readonly ColumnCollection columns;

			public ReadOnlyTableInfo(TableInfo baseTableInfo)
				: base(baseTableInfo.TableName) {
				columns = new ColumnCollection(this, Copy(this, baseTableInfo.Columns));
			}

			public override bool IsReadOnly => true;

			public override IColumnList Columns => columns;

			private static IEnumerable<ColumnInfo> Copy(TableInfo tableInfo, IEnumerable<ColumnInfo> columns) {
				return columns.Select(x => 
					new ColumnInfo(x.ColumnName, x.ColumnType, x.DefaultValue){TableInfo = tableInfo});
			}
		}

		#endregion

		#region ColumnResolver

		class ColumnResolver : SqlExpressionVisitor {
			private readonly TableInfo tableInfo;
			private readonly bool ignoreCase;

			public ColumnResolver(TableInfo tableInfo, bool ignoreCase) {
				this.tableInfo = tableInfo;
				this.ignoreCase = ignoreCase;
			}

			public override SqlExpression VisitReference(SqlReferenceExpression expression) {
				var comparison = ignoreCase ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;

				var refName = expression.ReferenceName;
				var tableName = refName.Parent;
				var columnName = refName.Name;

				if (tableName != null &&
				    !tableInfo.TableName.Equals(tableName, ignoreCase))
					return expression;

				foreach (var column in tableInfo.Columns) {
					if (String.Equals(column.ColumnName, columnName, comparison))
						return SqlExpression.Reference(column.FullName);
				}

				return base.VisitReference(expression);
			}
		}

		#endregion
	}
}