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
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Sql.Views {
	[Serializable]
	public sealed class ViewInfo : IObjectInfo, ISerializable {
		public ViewInfo(TableInfo tableInfo, SqlQueryExpression queryExpression, IQueryPlanNode queryPlan) {
			if (tableInfo == null)
				throw new ArgumentNullException("tableInfo");
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");

			TableInfo = tableInfo;
			QueryExpression = queryExpression;
			QueryPlan = queryPlan;
		}

		private ViewInfo(ObjectData data) {
			TableInfo = data.GetValue<TableInfo>("TableInfo");
			QueryExpression = data.GetValue<SqlQueryExpression>("QueryExpression");
			QueryPlan = data.GetValue<IQueryPlanNode>("QueryPlan");

		}

		public TableInfo TableInfo { get; private set; }

		public ObjectName ViewName {
			get { return TableInfo.TableName; }
		}

		public SqlQueryExpression QueryExpression { get; private set; }

		public IQueryPlanNode QueryPlan { get; private set; }

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.View; }
		}

		ObjectName IObjectInfo.FullName {
			get { return ViewName; }
		}

		void ISerializable.GetData(SerializeData data) {
			data.SetValue("TableInfo", TableInfo);
			data.SetValue("QueryPlan", QueryPlan);
			data.SetValue("QueryExpression", QueryExpression);
		}

		public static void Serialize(ViewInfo viewInfo, BinaryWriter writer) {
			var serializer = new BinarySerializer();
			serializer.Serialize(writer, viewInfo);
		}

		public static ViewInfo Deserialize(Stream stream) {
			var serializer = new BinarySerializer();
			return (ViewInfo) serializer.Deserialize(stream, typeof (ViewInfo));
		}

		//public static void Serialize(ViewInfo viewInfo, BinaryWriter writer) {
		//	TableInfo.Serialize(viewInfo.TableInfo, writer);
		//	SqlExpression.Serialize(viewInfo.QueryExpression, writer);

		//	var queryPlanType = viewInfo.QueryPlan.GetType();
		//	writer.Write(queryPlanType.FullName);
		//	QueryPlanSerializers.Serialize(viewInfo.QueryPlan, writer);
		//}

		//public static ViewInfo Deserialize(Stream stream, ITypeResolver resolver) {
		//	var reader = new BinaryReader(stream, Encoding.Unicode);
		//	return Deserialize(reader, resolver);
		//}

		//public static ViewInfo Deserialize(BinaryReader reader, ITypeResolver typeResolver) {
		//	var tableInfo = TableInfo.Deserialize(reader, typeResolver);
		//	var expression = SqlExpression.Deserialize(reader);

		//	if (!(expression is SqlQueryExpression))
		//		throw new InvalidOperationException();

		//	var queryExpression = (SqlQueryExpression) expression;

		//	var queryPlanTypeString = reader.ReadString();
		//	var queryPlanType = Type.GetType(queryPlanTypeString, true);
		//	var queryPlan = QueryPlanSerializers.Deserialize(queryPlanType, reader);

		//	return new ViewInfo(tableInfo, queryExpression, queryPlan);
		//}

		public SqlBinary AsBinary() {
			using (var stream = new MemoryStream()) {
				using (var writer = new BinaryWriter(stream, Encoding.Unicode)) {
					var serializer = new BinarySerializer();
					serializer.Serialize(writer, this);
					writer.Flush();
				}

				var data = stream.ToArray();
				return new SqlBinary(data);
			}
		}
	}
}
