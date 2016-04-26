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
using System.Linq;

namespace Deveel.Data.Sql.Compile {
	public static class Name {
		[CLSCompliant(false)]
		public static ObjectName Object(PlSqlParser.ObjectNameContext context) {
			if (context == null)
				return null;

			var parts = context.id().Select(x => x.GetText()).ToArray();
			var realParts = new List<string>();

			foreach (var part in parts) {
				if (!String.IsNullOrEmpty(part)) {
					var sp = part.Split('.');
					foreach (var s in sp) {
						realParts.Add(s);
					}
				}
			}

			parts = realParts.ToArray();

			ObjectName name = null;

			for (int i = 0; i < parts.Length; i++) {
				var part = InputString.AsNotQuoted(parts[i]);

				if (name == null) {
					name = new ObjectName(part);
				} else {
					name = new ObjectName(name, part);
				}
			}

			return name;
		}

		[CLSCompliant(false)]
		public static string Simple(PlSqlParser.IdContext context) {
			if (context == null)
				return null;

			return InputString.AsNotQuoted(context.GetText());
		}

		[CLSCompliant(false)]
		public static string Simple(PlSqlParser.Column_aliasContext context) {
			return Simple(context.id());
		}

		[CLSCompliant(false)]
		public static string Simple(PlSqlParser.LabelNameContext context) {
			return Simple(context.id());
		}

		[CLSCompliant(false)]
		public static string Simple(PlSqlParser.UserNameContext context) {
			if (context == null)
				return null;

			return Simple(context.id());
		}

		[CLSCompliant(false)]
		public static string Simple(PlSqlParser.GranteeNameContext context) {
			if (context.userName() != null)
				return Simple(context.userName());

			return Simple(context.roleName());
		}

		[CLSCompliant(false)]
		public static string Simple(PlSqlParser.RoleNameContext context) {
			if (context == null)
				return null;

			return Simple(context.id());
		}

		[CLSCompliant(false)]
		public static string Simple(PlSqlParser.ColumnNameContext context) {
			if (context == null)
				return null;

			return Simple(context.id());
		}

		[CLSCompliant(false)]
		public static string Simple(PlSqlParser.Cursor_nameContext context) {
			if (context == null)
				return null;

			return Simple(context.id());
		}

		[CLSCompliant(false)]
		public static string Simple(PlSqlParser.Regular_idContext context) {
			if (context == null)
				return null;

			return context.GetText();
		}

		[CLSCompliant(false)]
		public static ObjectName Select(PlSqlParser.ObjectNameContext context, bool glob) {
			var name = Object(context);
			if (glob)
				name = new ObjectName(name, "*");
			return name;
		}

		[CLSCompliant(false)]
		public static string Variable(PlSqlParser.Bind_variableContext context) {
			var text = context.GetText();
			if (String.IsNullOrEmpty(text))
				return text;

			if (text[0] == ':')
				text = text.Substring(1);

			return text;
		}

		[CLSCompliant(false)]
		public static string Variable(PlSqlParser.Variable_nameContext context) {
			if (context.bind_variable() != null)
				return Variable(context.bind_variable());

			return Simple(context.id());
		}

		[CLSCompliant(false)]
		public static string Simple(PlSqlParser.Parameter_nameContext context) {
			return Simple(context.id());
		}

		[CLSCompliant(false)]
		public static string Simple(PlSqlParser.Variable_nameContext context) {
			return Simple(context.id());
		}
	}
}
