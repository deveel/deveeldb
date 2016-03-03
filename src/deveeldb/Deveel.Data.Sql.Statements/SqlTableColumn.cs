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

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Statements {
	[Serializable]
	public sealed class SqlTableColumn : IPreparable, ISerializable {
		public SqlTableColumn(string columnName, SqlType columnType) {
			if (String.IsNullOrEmpty(columnName))
				throw new ArgumentNullException("columnName");
			if (columnType == null)
				throw new ArgumentNullException("columnType");
			
			ColumnName = columnName;
			ColumnType = columnType;
		}

		private SqlTableColumn(ObjectData data) {
			ColumnName = data.GetString("ColumnName");
			ColumnType = data.GetValue<SqlType>("ColumnType");
			IsIdentity = data.GetBoolean("IsIdentity");
			IsNotNull = data.GetBoolean("IsNotNull");
			DefaultExpression = data.GetValue<SqlExpression>("Default");
		}

		public string ColumnName { get; private set; }

		public SqlType ColumnType { get; private set; }

		public bool IsIdentity { get; set; }

		public SqlExpression DefaultExpression { get; set; }

		public bool HasDefaultExpression {
			get { return DefaultExpression != null; }
		}

		public bool IsNotNull { get; set; }

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var column = new SqlTableColumn(ColumnName, ColumnType);
			if (DefaultExpression != null)
				column.DefaultExpression = DefaultExpression.Prepare(preparer);

			column.IsNotNull = IsNotNull;
			return column;
		}

		//public static SqlTableColumn Deserialize(BinaryReader reader) {
		//	// TODO: Type resolver!!
		//	var columnName = reader.ReadString();
		//	var columnType = TypeSerializer.Deserialize(reader, null);
		//	var notNull = reader.ReadBoolean();
		//	var identity = reader.ReadBoolean();
		//	var defaultExpression = SqlExpression.Deserialize(reader);
		//	return new SqlTableColumn(columnName, columnType) {
		//		IsNotNull = notNull,
		//		IsIdentity = identity,
		//		DefaultExpression = defaultExpression
		//	};
		//}

		//public static void Serialize(SqlTableColumn column, BinaryWriter writer) {
		//	writer.Write(column.ColumnName);
		//	TypeSerializer.SerializeTo(writer, column.ColumnType);
		//	writer.Write(column.IsNotNull);
		//	writer.Write(column.IsIdentity);
		//	SqlExpression.Serialize(column.DefaultExpression, writer);
		//}

		void ISerializable.GetData(SerializeData data) {
			data.SetValue("ColumnName", ColumnName);
			data.SetValue("ColumnType", ColumnType);
			data.SetValue("IsNotNull", IsNotNull);
			data.SetValue("IsIdentity", IsIdentity);
			data.SetValue("Default", DefaultExpression);
		}
	}
}
