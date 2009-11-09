using System;

namespace Deveel.Data.DbModel {
	public enum DbConstraintType {
		PrimaryKey,
		Unique,
		Check,
		ForeignKey
	}
}