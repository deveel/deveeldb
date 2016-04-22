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

namespace Deveel.Data.Sql.Types {
	[Serializable]
	public sealed class UserTypeMember : ISerializable {
		public UserTypeMember(string memberName, SqlType memberType) {
			if (String.IsNullOrEmpty(memberName))
				throw new ArgumentNullException("memberName");
			if (memberType == null)
				throw new ArgumentNullException("memberType");

			MemberName = memberName;
			MemberType = memberType;
		}

		private UserTypeMember(SerializationInfo info, StreamingContext context) {
			MemberName = info.GetString("Name");
			MemberType = (SqlType) info.GetValue("Type", typeof(SqlType));
		}

		public string MemberName { get; private set; }

		public SqlType MemberType { get; private set; }

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Name", MemberName);
			info.AddValue("Type", MemberType);
		}
	}
}
