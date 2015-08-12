using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Deveel.Data.DbSystem;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class DropViewStatement : SqlStatement {
		public DropViewStatement(string[] viewNames) 
			: this(viewNames, false) {
		}

		public DropViewStatement(string[] viewNames, bool ifExists) {
			ViewNames = viewNames;
			IfExists = ifExists;
		}

		public string[] ViewNames { get; private set; }

		public bool IfExists { get; set; }

		protected override SqlStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			var viewNameList = ViewNames.ToList();
			var dropViews = new List<string>();

			foreach (var tableName in viewNameList) {
				if (dropViews.Contains(tableName, StringComparer.OrdinalIgnoreCase))
					throw new StatementPrepareException(String.Format("Duplicated table name '{0}' in the list of tables to drop.",
						tableName));

				dropViews.Add(tableName);
			}

			var resolvedNames = dropViews.Select(context.ResolveObjectName);

			return new Prepared(resolvedNames.ToArray(), IfExists);
		}

		#region Prepared

		internal class Prepared : SqlStatement {
			public ObjectName[] ViewNames { get; set; }

			public bool IfExists { get; set; }

			public Prepared(ObjectName[] viewNames, bool ifExists) {
				ViewNames = viewNames;
				IfExists = ifExists;
			}

			protected override bool IsPreparable {
				get { return false; }
			}

			protected override ITable ExecuteStatement(IQueryContext context) {
				context.DropViews(ViewNames, IfExists);
				return FunctionTable.ResultTable(context, 0);
			}
		}

		#endregion

		#region PreparedSerializer

		internal class PreparedSerializer : ObjectBinarySerializer<Prepared> {
			public override void Serialize(Prepared obj, BinaryWriter writer) {
				var namesLength = obj.ViewNames.Length;
				writer.Write(namesLength);
				for (int i = 0; i < namesLength; i++) {
					ObjectName.Serialize(obj.ViewNames[i], writer);
				}

				writer.Write(obj.IfExists);
			}

			public override Prepared Deserialize(BinaryReader reader) {
				var namesLength = reader.ReadInt32();
				var viewNames = new ObjectName[namesLength];
				for (int i = 0; i < namesLength; i++) {
					viewNames[i] = ObjectName.Deserialize(reader);
				}

				var ifExists = reader.ReadBoolean();

				return new Prepared(viewNames, ifExists);
			}
		}

		#endregion
	}
}
