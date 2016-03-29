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
using System.Collections.Generic;

using Deveel.Data.Sql;

namespace Deveel.Data.Sql.Types {
	public class UserTypeInfo : IObjectInfo {
		private readonly Dictionary<string, UserTypeMember> memberNamesCache;
		private List<UserTypeMember> members; 

		public UserTypeInfo(ObjectName typeName) 
			: this(typeName, null) {
		}

		public UserTypeInfo(ObjectName typeName, ObjectName parentType) {
			if (typeName == null)
				throw new ArgumentNullException("typeName");

			TypeName = typeName;
			ParentType = parentType;

			memberNamesCache = new Dictionary<string, UserTypeMember>();
		}

		public ObjectName TypeName { get; private set; }

		public bool IsSealed { get; set; }

		public ObjectName ParentType { get; private set; }

		public UserTypeMember this[int offset] {
			get { return members[offset]; }
		}

		public UserTypeMember FindMember(string name) {
			UserTypeMember member;
			if (!memberNamesCache.TryGetValue(name, out member)) {
				foreach (var typeMember in members) {
					if (typeMember.MemberName.Equals(name, StringComparison.OrdinalIgnoreCase)) {
						memberNamesCache[typeMember.MemberName] = typeMember;
						member = typeMember;
						break;
					}
				}
			}

			return member;
		}

		public int IndexOfMember(string name) {
			for (int i = 0; i < members.Count; i++) {
				var typeMember = members[i];
				if (typeMember.MemberName.Equals(name, StringComparison.OrdinalIgnoreCase)) {
					return i;
				}
			}

			return -1;
		}

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.Type; }
		}

		ObjectName IObjectInfo.FullName {
			get { return TypeName; }
		}
	}
}
