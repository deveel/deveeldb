using System;

namespace Deveel.Data.DbModel {
	public enum DbObjectType {
		Database,
		Schema,
		Table,
		View,
		Column,
		Function,
		DataType,
		Constraint,
		Privilege
	}
}