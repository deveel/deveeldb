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
using System.Reflection;

using Deveel.Data.Types;

namespace Deveel.Data.Mapping {
	public sealed class MemberMapping {
		public MemberMapping(MemberInfo member, string columnName, SqlType columnType) {
			if (member == null)
				throw new ArgumentNullException("member");

			Member = member;
			ColumnName = columnName;
			ColumnType = columnType;
		}

		public string ColumnName { get; private set; }

		public MemberInfo Member { get; private set; }

		public string MemberName {
			get { return Member.Name; }
		}

		public Type MemberType {
			get {
				if (Member is PropertyInfo)
					return ((PropertyInfo) Member).PropertyType;
				if (Member is FieldInfo)
					return ((FieldInfo) Member).FieldType;

				throw new InvalidOperationException("Invalid member");
			}
		}

		public SqlType ColumnType { get; private set; }

		public bool IsNotNull { get; set; }

		public static MemberMapping CreateFrom(MemberInfo member, ITypeMappingContext mappingContext) {
			throw new NotImplementedException();
		}
	}
}
