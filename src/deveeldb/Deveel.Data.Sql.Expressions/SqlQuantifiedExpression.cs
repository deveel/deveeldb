using System;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Sql.Expressions {
	[Serializable]
	public sealed class SqlQuantifiedExpression : SqlExpression {
		private readonly SqlExpressionType type;

		internal SqlQuantifiedExpression(SqlExpressionType type, SqlExpression value) {
			if (type != SqlExpressionType.All &&
				type != SqlExpressionType.Any)
				throw new ArgumentException("Invalid quantified type");

			if (value == null)
				throw new ArgumentNullException("value");

			this.type = type;
			ValueExpression = value;
		}

		private SqlQuantifiedExpression(SerializationInfo info, StreamingContext context)
			: base(info, context) {
			ValueExpression = (SqlExpression) info.GetValue("Value", typeof (SqlExpression));
			type = (SqlExpressionType) info.GetInt32("Type");
		}

		public override SqlExpressionType ExpressionType {
			get { return type; }
		}

		public SqlExpression ValueExpression { get; private set; }

		public bool IsArrayValue {
			get {
				return ValueExpression.ExpressionType == SqlExpressionType.Constant &&
				       ((SqlConstantExpression) ValueExpression).Value.Type is ArrayType;
			}
		}

		public bool IsTupleValue {
			get { return ValueExpression.ExpressionType == SqlExpressionType.Tuple; }
		}

		public bool IsSubQueryValue {
			get { return ValueExpression.ExpressionType == SqlExpressionType.Query; }
		}

		protected override void GetData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Type", type);
			info.AddValue("Value", ValueExpression);
			base.GetData(info, context);
		}
	}
}
