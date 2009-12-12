using System;

using Deveel.Data.Sql;

namespace Deveel.Data.Procedures {
	/// <summary>
	/// The parameter definition of a <see cref="StoredProcedure"/>.
	/// </summary>
	public sealed class ProcedureParameter : IStatementTreeObject {
		public ProcedureParameter(string name, TType type, ParameterDirection direction, bool nullable) {
			if (name == null)
				throw new ArgumentNullException("name");

			this.name = name;
			this.type = type;
			this.direction = direction;
			this.nullable = nullable;
		}

		private readonly string name;
		private readonly TType type;
		private ParameterDirection direction;
		private bool nullable;

		#region Implementation of ICloneable

		public bool IsNullable {
			get { return nullable; }
			set { nullable = value; }
		}

		public ParameterDirection Direction {
			get { return direction; }
			set { direction = value; }
		}

		public TType Type {
			get { return type; }
		}

		public string Name {
			get { return name; }
		}

		public object Clone() {
			throw new NotImplementedException();
		}

		public void PrepareExpressions(IExpressionPreparer preparer) {
			throw new NotImplementedException();
		}

		#endregion
	}
}