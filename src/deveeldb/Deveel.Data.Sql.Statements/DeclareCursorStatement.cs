using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Query;

namespace Deveel.Data.Sql.Statements {
	public sealed class DeclareCursorStatement : SqlStatement {
		public DeclareCursorStatement(string cursorName, SqlQueryExpression queryExpression) 
			: this(cursorName, null, queryExpression) {
		}

		public DeclareCursorStatement(string cursorName, IEnumerable<CursorParameter> parameters, SqlQueryExpression queryExpression) 
			: this(cursorName, parameters, CursorFlags.Insensitive, queryExpression) {
		}

		public DeclareCursorStatement(string cursorName, CursorFlags flags, SqlQueryExpression queryExpression) 
			: this(cursorName, null, flags, queryExpression) {
		}

		public DeclareCursorStatement(string cursorName, IEnumerable<CursorParameter> parameters, CursorFlags flags, SqlQueryExpression queryExpression) {
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");
			if (String.IsNullOrEmpty(cursorName))
				throw new ArgumentNullException("cursorName");

			CursorName = cursorName;
			Parameters = parameters;
			Flags = flags;
			QueryExpression = queryExpression;
		}

		protected override bool IsPreparable {
			get { return false; }
		}

		public string CursorName { get; private set; }

		public SqlQueryExpression QueryExpression { get; private set; }

		public CursorFlags Flags { get; set; }

		public IEnumerable<CursorParameter> Parameters { get; set; } 

		protected override ITable ExecuteStatement(IQueryContext context) {
			var cursorInfo = new CursorInfo(CursorName, Flags, QueryExpression);
			if (Parameters != null) {
				foreach (var parameter in Parameters) {
					cursorInfo.Parameters.Add(parameter);
				}
			}

			context.DeclareCursor(cursorInfo);
			return FunctionTable.ResultTable(context, 0);
		}

		#region Serializer

		internal class Serializer : ObjectBinarySerializer<DeclareCursorStatement> {
			public override void Serialize(DeclareCursorStatement obj, BinaryWriter writer) {
				writer.Write(obj.CursorName);
				writer.Write((byte)obj.Flags);

				if (obj.Parameters != null) {
					var pars = obj.Parameters.ToArray();
					var parLength = pars.Length;
					writer.Write(parLength);

					for (int i = 0; i < parLength; i++) {
						CursorParameter.Serialize(pars[i], writer);
					}
				} else {
					writer.Write(0);
				}

				SqlExpression.Serialize(obj.QueryExpression, writer);
			}

			public override DeclareCursorStatement Deserialize(BinaryReader reader) {
				var cursorName = reader.ReadString();
				var flags = (CursorFlags) reader.ReadByte();

				var pars = new List<CursorParameter>();
				var parLength = reader.ReadInt32();
				for (int i = 0; i < parLength; i++) {
					var param = CursorParameter.Deserialize(reader);
					pars.Add(param);
				}

				var queryExpression = SqlExpression.Deserialize(reader) as SqlQueryExpression;

				return new DeclareCursorStatement(cursorName, pars.ToArray(), flags, queryExpression);
			}
		}

		#endregion
	}
}
