using System;
using System.IO;

using Deveel.Data.DbSystem;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Cursors;

namespace Deveel.Data.Sql.Statements {
	public sealed class CloseStatement : SqlStatement {
		public CloseStatement(string cursorName) {
			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");

			CursorName = cursorName;
		}

		public string CursorName { get; private set; }

		protected override bool IsPreparable {
			get { return false; }
		}

		protected override ITable ExecuteStatement(IQueryContext context) {
			context.CloseCursor(CursorName);
			return FunctionTable.ResultTable(context, 0);
		}

		#region Serializer

		internal class Serializer : ObjectBinarySerializer<CloseStatement> {
			public override void Serialize(CloseStatement obj, BinaryWriter writer) {
				writer.Write(obj.CursorName);
			}

			public override CloseStatement Deserialize(BinaryReader reader) {
				var cursorName = reader.ReadString();
				return new CloseStatement(cursorName);
			}
		}

		#endregion
	}
}
