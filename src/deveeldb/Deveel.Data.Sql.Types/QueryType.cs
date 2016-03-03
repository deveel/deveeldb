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
using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Types {
	[Serializable]
	public sealed class QueryType : SqlType {
		public QueryType()
			: base("QUERY", SqlTypeCode.QueryPlan) {
		}

		private QueryType(ObjectData data)
			: base(data) {
		}

		public override bool IsIndexable {
			get { return false; }
		}

		public override bool IsStorable {
			get { return false; }
		}

		public override ISqlObject DeserializeObject(Stream stream) {
			var reader = new BinaryReader(stream, Encoding.Unicode);
			var isNull = reader.ReadByte() == 0;
			if (isNull)
				return SqlQueryObject.Null;

			var nodeTypeString = reader.ReadString();
			var nodeType = Type.GetType(nodeTypeString, true);
			var queryPlan = DeserializePlan(nodeType, reader);

			return new SqlQueryObject(queryPlan);
		}

		private static IQueryPlanNode DeserializePlan(Type nodeType, BinaryReader reader) {
			var serializer = new BinarySerializer();
			return (IQueryPlanNode) serializer.Deserialize(reader, nodeType);
		}

		public override void SerializeObject(Stream stream, ISqlObject obj) {
			var writer = new BinaryWriter(stream, Encoding.Unicode);

			var queryPlanObj = (SqlQueryObject) obj;
			if (queryPlanObj.IsNull) {
				writer.Write((byte) 0);
			} else {
				writer.Write((byte)1);
				var nodeTypeString = queryPlanObj.QueryPlan.GetType().AssemblyQualifiedName;
				if (String.IsNullOrEmpty(nodeTypeString))
					throw new InvalidOperationException();

				writer.Write(nodeTypeString);
				SerializePlan(queryPlanObj.QueryPlan, writer);
			}
		}

		private static void SerializePlan(IQueryPlanNode queryPlan, BinaryWriter writer) {
			var serializer = new BinarySerializer();
			serializer.Serialize(writer, queryPlan);
		}
	}
}