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

using Deveel.Data.Serialization;
using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	// TODO: Make it disposable?
	[Serializable]
	public sealed class QueryParameter : ISerializable {
		public QueryParameter(SqlType sqlType) 
			: this(sqlType, null) {
		}

		public QueryParameter(SqlType sqlType, ISqlObject value) 
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
				name[0] != NamePrefix)
				throw new ArgumentException(String.Format("The parameter name '{0}' is invalid: must be '{1}' or starting with '{2}'", name, Marker, NamePrefix));

			Name = name;
			SqlType = sqlType;
			Value = value;
			Direction = QueryParameterDirection.In;
		}

		private QueryParameter(ObjectData data) {
			Name = data.GetString("Name");
			SqlType = data.GetValue<SqlType>("Type");
			Value = data.GetValue<ISqlObject>("Value");
			Direction = (QueryParameterDirection) data.GetInt32("Direction");
		}

		public const char NamePrefix = ':';
		public const string Marker = "?";

		public string Name { get; private set; }

		public SqlType SqlType { get; private set; }

		public QueryParameterDirection Direction { get; set; }

		public ISqlObject Value { get; set; }

		void ISerializable.GetData(SerializeData data) {
			data.SetValue("Name", Name);
			data.SetValue("Type", SqlType);
			data.SetValue("Direction", Direction);
			data.SetValue("Value", Value);
		}
	}
}