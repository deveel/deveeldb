// 
//  Copyright 2010-2014 Deveel
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

using System;

namespace Deveel.Data.Sql {
	/// <summary>
	/// The intermediate object used to analyze and parse
	/// a cursor FETCH statement.
	/// </summary>
	[Serializable]
	internal class CursorFetch : IStatementTreeObject {
		private Expression offset;
		private SelectIntoClause into = new SelectIntoClause();
		private FetchOrientation orientation;

		public Expression Offset {
			get { return offset; }
			set { offset = value; }
		}

		public FetchOrientation Orientation {
			get { return orientation; }
			set { orientation = value; }
		}

		public SelectIntoClause Into {
			get { return into; }
		}

		#region Implementation of ICloneable

		public object Clone() {
			CursorFetch fetch = new CursorFetch();
			if (offset != null)
				fetch.offset = (Expression) offset.Clone();
			fetch.into = (SelectIntoClause) into.Clone();
			return fetch;
		}

		void IStatementTreeObject.PrepareExpressions(IExpressionPreparer preparer) {
			if (offset != null)
				offset.Prepare(preparer);
		}

		#endregion
	}
}