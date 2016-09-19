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
using System.Reflection;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;

using DryIoc;

namespace Deveel.Data.Mapping {
	public sealed class TypeMapInfo {
		private readonly List<MemberMapInfo> members;

		internal TypeMapInfo(Type type, ObjectName tableName) {
			Type = type;
			TableName = tableName;
			members = new List<MemberMapInfo>();
		}

		public Type Type { get; private set; }

		public ObjectName TableName { get; private set; }

		public IEnumerable<MemberMapInfo> Members {
			get { return members.AsEnumerable(); }
		}

		public IEnumerable<SqlTableColumn> Columns {
			get { return Members.Select(x => x.AsTableColumn()); }
		}

		public IEnumerable<ConstraintMapInfo> Constraints {
			get { return Members.Where(x => x.Constraints != null).SelectMany(x => x.Constraints); }
		}

		internal void AddMember(MemberMapInfo member) {
			members.Add(member);
		}

		public object ToObject(Row row) {
#if PCL
			var ctor = Type.GetConstructorOrNull(true);
#else
			var ctor = Type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
				.FirstOrDefault(x => x.GetParameters().Length == 0);
#endif

			if (ctor == null)
				throw new InvalidOperationException(String.Format("The type '{0}' has no default constructor.", Type));

			var obj = ctor.Invoke(new object[0]);
			foreach (var member in members) {
				member.SetValue(row, obj);
			}

			return obj;
		}
	}
}