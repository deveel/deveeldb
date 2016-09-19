using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Mapping {
	public sealed class TypeConstraintMapInfo {
		internal TypeConstraintMapInfo(ConstraintType constraintType, IEnumerable<TypeMemberMapInfo> members, SqlExpression checkExpression) {
			ConstraintType = constraintType;
			Members = members;
			CheckExpression = checkExpression;
		}

		public ConstraintType ConstraintType { get; private set; }

		public IEnumerable<TypeMemberMapInfo> Members { get; private set; }

		public SqlExpression CheckExpression { get; private set; }

		public bool IsCheck {
			get { return ConstraintType == ConstraintType.Check; }
		}

		public bool IsPrimaryKey {
			get { return ConstraintType == ConstraintType.PrimaryKey;  }
		}

		public bool IsUnique {
			get { return ConstraintType == ConstraintType.Unique; }
		}
	}
}
