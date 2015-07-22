using System;
using System.Data;
using System.Data.Common;

namespace Deveel.Data.Client {
	public sealed class DeveelDbParameter : DbParameter {
		public override void ResetDbType() {
			throw new NotImplementedException();
		}

		public override DbType DbType { get; set; }

		public override System.Data.ParameterDirection Direction { get; set; }

		public override bool IsNullable { get; set; }

		public override string ParameterName { get; set; }

		public override string SourceColumn { get; set; }

		public override DataRowVersion SourceVersion { get; set; }

		public override object Value { get; set; }

		public override bool SourceColumnNullMapping { get; set; }

		public override int Size { get; set; }
	}
}
