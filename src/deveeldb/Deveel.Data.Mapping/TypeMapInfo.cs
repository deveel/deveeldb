using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Deveel.Data.Sql;
using Deveel.Data.Sql.Statements;
using Deveel.Data.Sql.Tables;

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
			get { return Members.Where(x => x.Constraint != null).Select(x => x.Constraint); }
		} 

		internal void AddMember(MemberMapInfo member) {
			members.Add(member);
		}

		public object ToObject(Row row) {
			var ctor = Type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
				.FirstOrDefault(x => x.GetParameters().Length == 0);

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
