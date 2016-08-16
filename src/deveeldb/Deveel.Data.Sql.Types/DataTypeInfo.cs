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

using Deveel.Data.Sql.Compile;

namespace Deveel.Data.Sql.Types {
	public sealed class DataTypeInfo {
		public DataTypeInfo(string typeName) 
			: this(typeName, new DataTypeMeta[0]) {
		}

		public DataTypeInfo(string typeName, DataTypeMeta[] metadata) {
			if (String.IsNullOrEmpty(typeName))
				throw new ArgumentNullException("typeName");

			TypeName = typeName;
			Metadata = metadata;
		}

		static DataTypeInfo() {
			DefaultParser = new DefaultDataTypeParser();
		}

		public string TypeName { get; private set; }

		public DataTypeMeta[] Metadata { get; private set; }

		public bool IsPrimitive {
			get { return PrimitiveTypes.IsPrimitive(TypeName); }
		}

		public static IDataTypeParser DefaultParser { get; private set; }

		public static DataTypeInfo Parse(string s) {
			return DefaultParser.Parse(s);
		}

		#region DefaultDataTypeParser

		class DefaultDataTypeParser : IDataTypeParser {
			public DataTypeInfo Parse(string s) {
				return new PlSqlCompiler().ParseDataType(s);
			}
		}

		#endregion
	}
}
