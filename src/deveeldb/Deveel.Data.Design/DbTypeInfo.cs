using System;
using System.Collections.Generic;
using System.Linq;

using Deveel.Data.Design.Configuration;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design {
	public sealed class DbTypeInfo {
		internal DbTypeInfo(TypeModelConfiguration configuration) {
			Configuration = configuration;
		}

		private TypeModelConfiguration Configuration { get; set; }

		public string TableName {
			get { return Configuration.TableName; }
		}

		public Type Type {
			get { return Configuration.Type; }
		}

		public DbMemberInfo GetMember(string memberName) {
			var buildInfo = Configuration.GetMember(memberName);
			if (buildInfo == null)
				return null;

			return new DbMemberInfo(buildInfo);
		}

		internal IEnumerable<DbConstraintInfo> GetConstraints() {
			return Configuration.GetConstraints().Select(x => new DbConstraintInfo(x));
		}

		internal object CreateObject(Row row) {
			if (row == null)
				throw new ArgumentNullException("row");

			// TODO: Construct parametrized objects

			var obj = Activator.CreateInstance(Type, true);

			foreach (var memberInfo in Configuration.MemberNames.Select(GetMember)) {
				memberInfo.ApplyFromRow(obj, row);
			}

			return obj;
		}
	}
}
