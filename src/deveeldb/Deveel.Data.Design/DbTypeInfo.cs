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

		public DbMemberInfo KeyMember {
			get {
				var key = GetConstraint(ConstraintType.PrimaryKey);
				if (key == null)
					return null;

				return key.Members.FirstOrDefault();
			}
		}

		public DbMemberInfo GetMember(string memberName) {
			var buildInfo = Configuration.GetMember(memberName);
			if (buildInfo == null)
				return null;

			return new DbMemberInfo(buildInfo);
		}

		public DbConstraintInfo GetConstraint(ConstraintType constraintType) {
			return GetConstraint(null, constraintType);
		}

		public DbConstraintInfo GetConstraint(string name, ConstraintType constraintType) {
			var config = Configuration.GetConstraint(name, constraintType);
			if (config == null)
				return null;

			return new DbConstraintInfo(config);
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
