using System;
using System.Linq;

namespace Deveel.Data.Sql.Compile {
	public static class Name {
		public static ObjectName Object(PlSqlParser.ObjectNameContext context) {
			if (context == null)
				return null;

			var parts = context.id().Select(x => x.GetText()).ToArray();
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

		public static string Simple(PlSqlParser.IdContext context) {
			return InputString.AsNotQuoted(context.GetText());
		}

		public static string Simple(PlSqlParser.Column_aliasContext context) {
			return Simple(context.id());
		}

		public static string Simple(PlSqlParser.LabelNameContext context) {
			return Simple(context.id());
		}

		public static string Simple(PlSqlParser.UserNameContext context) {
			if (context == null)
				return null;

			return Simple(context.id());
		}

		public static string Simple(PlSqlParser.GranteeNameContext context) {
			if (context.userName() != null)
				return Simple(context.userName());

			return Simple(context.roleName());
		}

		public static string Simple(PlSqlParser.RoleNameContext context) {
			if (context == null)
				return null;

			return Simple(context.id());
		}

		public static string Simple(PlSqlParser.ColumnNameContext context) {
			if (context == null)
				return null;

			return Simple(context.id());
		}

		public static string Simple(PlSqlParser.Cursor_nameContext context) {
			if (context == null)
				return null;

			return Simple(context.id());
		}

		public static string Simple(PlSqlParser.Regular_idContext context) {
			if (context == null)
				return null;

			return context.GetText();
		}

		public static ObjectName Select(PlSqlParser.ObjectNameContext context, bool glob) {
			var name = Object(context);
			if (glob)
				name = new ObjectName(name, "*");
			return name;
		}

		public static string Variable(PlSqlParser.Bind_variableContext context) {
			var text = context.GetText();
			if (String.IsNullOrEmpty(text))
				return text;

			if (text[0] == ':')
				text = text.Substring(1);

			return text;
		}

		public static string Variable(PlSqlParser.Variable_nameContext context) {
			if (context.bind_variable() != null)
				return Variable(context.bind_variable());

			return Simple(context.id());
		}

		public static string Simple(PlSqlParser.Parameter_nameContext context) {
			return Simple(context.id());
		}

		public static string Simple(PlSqlParser.Variable_nameContext context) {
			return Simple(context.id());
		}
	}
}
