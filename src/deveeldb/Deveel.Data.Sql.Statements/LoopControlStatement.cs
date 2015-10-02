// 
//  Copyright 2010-2015 Deveel
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//        http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//

using System;
using System.IO;

using Deveel.Data;
using Deveel.Data.Serialization;
using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Statements {
	public sealed class LoopControlStatement : SqlStatement {
		public LoopControlStatement(LoopControlType controlType) 
			: this(controlType, (SqlExpression) null) {
		}

		public LoopControlStatement(LoopControlType controlType, SqlExpression whenExpression) 
			: this(controlType, null, whenExpression) {
		}

		public LoopControlStatement(LoopControlType controlType, string label) 
			: this(controlType, label, null) {
		}

		public LoopControlStatement(LoopControlType controlType, string label, SqlExpression whenExpression) {
			Label = label;
			WhenExpression = whenExpression;
			ControlType = controlType;
		}

		public LoopControlType ControlType { get; private set; }

		public string Label { get; set; }

		public SqlExpression WhenExpression { get; set; }

		protected override bool IsPreparable {
			get { return true; }
		}

		protected override SqlStatement PrepareStatement(IExpressionPreparer preparer, IQueryContext context) {
			var label = Label;
			var whenExp = WhenExpression;
			if (whenExp != null)
				whenExp = whenExp.Prepare(preparer);

			return new Prepared(ControlType, label, whenExp);
		}

		#region Prepared

		internal class Prepared : SqlStatement {
			public Prepared(LoopControlType controlType, string label, SqlExpression whenExpression) {
				ControlType = controlType;
				Label = label;
				WhenExpression = whenExpression;
			}

			public LoopControlType ControlType { get; private set; }

			public string Label { get; private set; }

			public SqlExpression WhenExpression { get; private set; }

			protected override bool IsPreparable {
				get { return false; }
			}

			protected override ITable ExecuteStatement(IQueryContext context) {
				throw new NotImplementedException();
			}
		}

		#endregion

		#region Serializer

		internal class Serializer : ObjectBinarySerializer<Prepared> {
			public override void Serialize(Prepared obj, BinaryWriter writer) {
				writer.Write((byte)obj.ControlType);
				writer.Write(obj.Label);

				if (obj.WhenExpression != null) {
					writer.Write(true);
					SqlExpression.Serialize(obj.WhenExpression, writer);
				} else {
					writer.Write(false);
				}
			}

			public override Prepared Deserialize(BinaryReader reader) {
				var controlType = (LoopControlType) reader.ReadByte();
				var label = reader.ReadString();
				SqlExpression whenExp = null;
				var hasExp = reader.ReadBoolean();
				if (hasExp)
					whenExp = SqlExpression.Deserialize(reader);

				return new Prepared(controlType, label, whenExp);
			}
		}

		#endregion
	}
}
