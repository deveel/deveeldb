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
using Deveel.Data.Sql.Cursors;
using Deveel.Data.Sql.Tables;

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
