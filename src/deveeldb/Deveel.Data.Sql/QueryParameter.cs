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
using System.Runtime.Serialization;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql {
	// TODO: Make it disposable?
	[Serializable]
	public sealed class QueryParameter : ISerializable {
		public QueryParameter(SqlType sqlType) 
			: this(sqlType, null) {
		}

		public QueryParameter(SqlType sqlType, Objects.ISqlObject value) 
			: this(Marker, sqlType, value) {
		}

		public QueryParameter(string name, SqlType sqlType) 
			: this(name, sqlType, null) {
		}

		public QueryParameter(string name, SqlType sqlType, ISqlObject value) {
			if (sqlType == null)
				throw new ArgumentNullException("sqlType");

			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			if (!String.Equals(name, Marker, StringComparison.Ordinal) &&
			    name[0] == NamePrefix) {
				name = name.Substring(1);

				if (String.IsNullOrEmpty(name))
					throw new ArgumentException("Cannot specify only the variable bind prefix as parameter.");
			}

			Name = name;
			SqlType = sqlType;
			Value = value;
			Direction = QueryParameterDirection.In;
		}

		private QueryParameter(SerializationInfo info, StreamingContext context) {
			Name = info.GetString("Name");
			SqlType = (SqlType)info.GetValue("Type", typeof(SqlType));
			Value = (ISqlObject) info.GetValue("Value", typeof(ISqlObject));
			Direction = (QueryParameterDirection) info.GetInt32("Direction");
		}

		public const char NamePrefix = ':';
		public const string Marker = "?";

		public string Name { get; private set; }

		public SqlType SqlType { get; private set; }

		public QueryParameterDirection Direction { get; set; }

		public Objects.ISqlObject Value { get; set; }

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Name", Name);
			info.AddValue("Type", SqlType, typeof(SqlType));
			info.AddValue("Direction", Direction);
			info.AddValue("Value", Value, typeof(ISqlObject));
		}
	}
}