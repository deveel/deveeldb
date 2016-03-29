// 
//  Copyright 2010-2016 Deveel
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
using System.Runtime.Serialization;

namespace Deveel.Data.Sql.Expressions {
	[Serializable]
	public sealed class JoinPart : ISerializable {
		internal JoinPart(JoinType joinType, ObjectName tableName, SqlExpression onExpression) {
			if (tableName == null) 
				throw new ArgumentNullException("tableName");

			OnExpression = onExpression;
			JoinType = joinType;
			TableName = tableName;
		}

		internal JoinPart(JoinType joinType, SqlQueryExpression subQuery, SqlExpression onExpression) {
			if (subQuery == null) 
				throw new ArgumentNullException("subQuery");

			OnExpression = onExpression;
			JoinType = joinType;
			SubQuery = subQuery;
		}

		private JoinPart(SerializationInfo info, StreamingContext context) {
			TableName = (ObjectName)info.GetValue("Table", typeof(ObjectName));
			SubQuery = (SqlQueryExpression)info.GetValue("SubQuery", typeof(SqlQueryExpression));
			JoinType = (JoinType)info.GetInt32("JoinType");
			OnExpression = (SqlExpression) info.GetValue("On", typeof(SqlExpression));
		}

		public JoinType JoinType { get; private set; }

		public ObjectName TableName { get; private set; }

		public SqlQueryExpression SubQuery { get; private set; }

		public SqlExpression OnExpression { get; private set; }

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Table", TableName, typeof(ObjectName));
			info.AddValue("SubQuery", SubQuery, typeof(SqlQueryExpression));
			info.AddValue("JoinType", (int)JoinType);
			info.AddValue("On", OnExpression, typeof(SqlExpression));
		}

		//public static void Serialize(JoinPart joinPart, BinaryWriter writer) {
		//	writer.Write((byte)joinPart.JoinType);

		//	if (joinPart.SubQuery != null) {
		//		writer.Write((byte)2);
		//		SqlExpression.Serialize(joinPart.SubQuery, writer);
		//	} else {
		//		writer.Write((byte)1);
		//		ObjectName.Serialize(joinPart.TableName, writer);
		//	}

		//	SqlExpression.Serialize(joinPart.OnExpression, writer);
		//}

		//public static JoinPart Deserialize(BinaryReader reader) {
		//	var joinType = (JoinType) reader.ReadByte();

		//	var joinSourceType = reader.ReadByte();

		//	SqlQueryExpression subQuery = null;
		//	ObjectName tableName = null;

		//	if (joinSourceType == 1) {
		//		tableName = ObjectName.Deserialize(reader);
		//	} else if (joinSourceType == 2) {
		//		subQuery = (SqlQueryExpression) SqlExpression.Deserialize(reader);
		//	} else {
		//		throw new FormatException();
		//	}

		//	var onExpression = SqlExpression.Deserialize(reader);

		//	if (joinSourceType == 1)
		//		return new JoinPart(joinType, tableName, onExpression);

		//	return new JoinPart(joinType, subQuery, onExpression);
		//}
	}
}