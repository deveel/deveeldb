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
using System.Globalization;

using Deveel.Data.Sql.Objects;
using Deveel.Data.Types;

namespace Deveel.Data.Sql {
	public sealed class QueryParameter {
		public QueryParameter(DataType dataType) 
			: this(dataType, null) {
		}

		public QueryParameter(DataType dataType, ISqlObject value) 
			: this(Marker, dataType, value) {
		}

		public QueryParameter(string name, DataType dataType) 
			: this(name, dataType, null) {
		}

		public QueryParameter(string name, DataType dataType, ISqlObject value) {
			if (dataType == null)
				throw new ArgumentNullException("dataType");

			if (String.IsNullOrEmpty(name))
				throw new ArgumentNullException("name");

			if (!String.Equals(name, Marker, StringComparison.Ordinal) &&
				name[0] != NamePrefix)
				throw new ArgumentException(String.Format("The parameter name '{0}' is invalid: must be '{1}' or starting with '{2}'", name, Marker, NamePrefix));

			Name = name;
			DataType = dataType;
			Value = value;
			Direction = QueryParameterDirection.In;
		}

		public const char NamePrefix = ':';
		public const string Marker = "?";

		public string Name { get; private set; }

		public DataType DataType { get; private set; }

		public QueryParameterDirection Direction { get; set; }

		public ISqlObject Value { get; set; }
	}
}