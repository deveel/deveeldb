// 
//  Copyright 2010-2016 Deveel
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
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Sql.Cursors {
	public sealed class CursorInfo : IObjectInfo {
		public CursorInfo(string cursorName, SqlQueryExpression queryExpression) 
			: this(cursorName, CursorFlags.Insensitive, queryExpression) {
		}

		public CursorInfo(string cursorName, CursorFlags flags, SqlQueryExpression queryExpression) {
			if (cursorName == null)
				throw new ArgumentNullException("cursorName");
			if (queryExpression == null)
				throw new ArgumentNullException("queryExpression");

			CursorName = cursorName;
			QueryExpression = queryExpression;
			Parameters = new ParameterCollection();

			Flags = flags;
		}

		public string CursorName { get; private set; }

		public ICollection<CursorParameter> Parameters { get; private set; }

		public CursorFlags Flags { get; set; }

		string IObjectInfo.Owner {
			get { return null; }
		}

		public bool IsInsensitive {
			get { return (Flags & CursorFlags.Insensitive) != 0; }
		}


		public bool IsScroll {
			get { return (Flags & CursorFlags.Scroll) != 0; }
		}

		public SqlQueryExpression QueryExpression { get; private set; }

		DbObjectType IObjectInfo.ObjectType {
			get { return DbObjectType.Cursor; }
		}

		ObjectName IObjectInfo.FullName {
			get { return new ObjectName(CursorName); }
		}

		#region ParameterCollection

		class ParameterCollection : Collection<CursorParameter> {
			protected override void InsertItem(int index, CursorParameter item) {
				if (Items.Any(x => x.ParameterName == item.ParameterName))
					throw new ArgumentException(String.Format("Argument '{0}' was already added to the collection.", item.ParameterName));

				if (item.Offset < 0) {
					item.Offset = index;
				} else {
					index = item.Offset;
				}

				base.InsertItem(index, item);
			}

			protected override void SetItem(int index, CursorParameter item) {
				item.Offset = index;
				base.SetItem(index, item);
			}
		}

		#endregion
	}
}
