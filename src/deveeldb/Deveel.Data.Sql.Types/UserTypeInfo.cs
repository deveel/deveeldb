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
using System.Linq;

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

		public bool IsAbstract { get; set; }

		public ObjectName ParentType { get; private set; }

		public string Owner { get; set; }

		public int MemberCount {
			get { return members == null ? 0 : members.Count; }
		}

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

		public UserTypeMember AddMember(string memberName, SqlType memberType) {
			if (String.IsNullOrEmpty(memberName))
				throw new ArgumentNullException("memberName");
			if (memberType == null)
				throw new ArgumentNullException("memberType");

			try {
				var member = new UserTypeMember(memberName, memberType);
				AddMember(member);
				return member;
			} finally {
				memberNamesCache.Clear();
			}
		}

		public void AddMember(UserTypeMember member) {
			if (member == null)
				throw new ArgumentNullException("member");

			if (members == null)
				members = new List<UserTypeMember>();

			if (members.ToDictionary(x => x.MemberName, y => y, StringComparer.OrdinalIgnoreCase)
				.ContainsKey(member.MemberName))
				throw new ArgumentException(String.Format("A member named '{0}' is already present in type '{1}'.",
					member.MemberName, member.MemberType));

			members.Add(member);
			memberNamesCache.Clear();
		}

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.Type; }
		}

		ObjectName IObjectInfo.FullName {
			get { return TypeName; }
		}

		internal UserTypeMember MemberAt(int offset) {
			if (offset < 0 || offset >= MemberCount)
				throw new ArgumentOutOfRangeException("offset", offset, String.Format("The member offset is out of range"));

			return members[offset];
		}
	}
}
