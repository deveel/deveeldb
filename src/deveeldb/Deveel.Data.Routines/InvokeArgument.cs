using System;
using System.Runtime.Serialization;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Routines {
	[Serializable]
	public sealed class InvokeArgument : ISerializable, IPreparable {
		public InvokeArgument(SqlExpression value) 
			: this(null, value) {
		}

		public InvokeArgument(string name, SqlExpression value) {
			if (value == null)
				throw new ArgumentNullException("value");

			Name = name;
			Value = value;
		}

		private InvokeArgument(SerializationInfo info, StreamingContext context) {
			Name = info.GetString("Name");
			Value = (SqlExpression) info.GetValue("Value", typeof(SqlExpression));
		}

		public string Name { get; private set; }

		public SqlExpression Value { get; private set; }

		public bool IsNamed {
			get { return !String.IsNullOrEmpty(Name); }
		}

		void ISerializable.GetObjectData(SerializationInfo info, StreamingContext context) {
			info.AddValue("Name", Name);
			info.AddValue("Value", Value);
		}

		object IPreparable.Prepare(IExpressionPreparer preparer) {
			var preparedValue = Value.Prepare(preparer);
			return new InvokeArgument(Name, preparedValue);
		}
	}
}
