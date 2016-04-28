using System;

namespace Deveel.Data.Linq.Expressions {
	public enum QueryExpressionType {
		Table = 1000,
		ClientJoin,
		Column,
		Select,
		Projection,
		Entity,
		Join,
		Aggregate,
		Scalar,
		Exists,
		In,
		Grouping,
		AggregateSubquery,
		IsNull,
		Between,
		RowCount,
		NamedValue,
		OuterJoined,
		Insert,
		Update,
		Delete,
		Batch,
		Function,
		Block,
		If,
		Declaration,
		Variable
	}
}
