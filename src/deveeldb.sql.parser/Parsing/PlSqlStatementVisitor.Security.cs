// 
//  Copyright 2010-2018 Deveel
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
using System.Linq;

using Antlr4.Runtime.Misc;

using Deveel.Data.Security;
using Deveel.Data.Sql.Statements;

namespace Deveel.Data.Sql.Parsing
{
	partial class PlSqlStatementVisitor
	{
		public override SqlStatement VisitCreateUserStatement(PlSqlParser.CreateUserStatementContext context) {
			var userName = context.userName().GetText();
			string arg;
			IUserIdentificationInfo id;

			if (context.byPassword() != null) {
				arg = SqlParseInputString.AsNotQuoted(context.byPassword().CHAR_STRING().GetText());
				id = new PasswordIdentificationInfo(arg);
			} else if (context.externalId() != null) {
				arg = SqlParseInputString.AsNotQuoted(context.externalId().CHAR_STRING().GetText());
				throw new NotSupportedException("EXTERNAL identification not supported yet");
			} else if (context.globalId() != null) {
				arg = SqlParseInputString.AsNotQuoted(context.globalId().CHAR_STRING().GetText());
				throw new NotSupportedException("GLOBAL identification not supported yet");
			} else {
				throw new ParseCanceledException("Invalid identification option");
			}

			AddStatement(context, new CreateUserStatement(userName, id));
			return base.VisitCreateUserStatement(context);
		}

		public override SqlStatement VisitDropUserStatement(PlSqlParser.DropUserStatementContext context) {
			var userName = context.userName().GetText();

			AddStatement(context, new DropUserStatement(userName));
			return base.VisitDropUserStatement(context);
		}

		public override SqlStatement VisitCreateRoleStatement(PlSqlParser.CreateRoleStatementContext context) {
			var roleName = SqlParseUtil.Name.Simple(context.regular_id());
			AddStatement(context, new CreateRoleStatement(roleName));

			return base.VisitCreateRoleStatement(context);
		}

		public override SqlStatement VisitDropRoleStatement(PlSqlParser.DropRoleStatementContext context) {
			var roleName = SqlParseUtil.Name.Simple(context.regular_id());
			AddStatement(context, new DropRoleStatement(roleName));

			return base.VisitDropRoleStatement(context);
		}

		public override SqlStatement VisitGrantPrivilegeStatement(PlSqlParser.GrantPrivilegeStatementContext context) {
			var privs = Privilege.None;

			if (context.ALL() != null) {
				privs = SqlPrivileges.TableAll;
			} else {
				var privNames = context.privilegeName().Select(x => x.GetText());
				foreach (var privName in privNames) {
					try {
						var priv = SqlPrivileges.Resolver.ResolvePrivilege(privName);
						privs += priv;
					} catch (Exception) {
						throw new ParseCanceledException("Invalid privilege specified.");
					}
				}
			}

			var withGrant = context.WITH() != null && context.GRANT() != null;
			var grantee = SqlParseUtil.Name.Simple(context.granteeName());
			var objectName = SqlParseUtil.Name.Object(context.objectName());

			AddStatement(context, new GrantObjectPrivilegesStatement(grantee, privs, objectName, withGrant, new string[0]));

			return base.VisitGrantPrivilegeStatement(context);
		}

		public override SqlStatement VisitGrantRoleStatement(PlSqlParser.GrantRoleStatementContext context) {
			// TODO: this is also the point in which is possible to set the system privileges to a user

			var grantee = SqlParseUtil.Name.Simple(context.granteeName());
			var roleNames = context.roleName().Select(SqlParseUtil.Name.Simple).ToArray();

			foreach (var roleName in roleNames) {
				AddStatement(context, new GrantRoleStatement(grantee, roleName));
			}

			return base.VisitGrantRoleStatement(context);
		}

		public override SqlStatement VisitRevokeRoleStatement(PlSqlParser.RevokeRoleStatementContext context) {
			var grantee = SqlParseUtil.Name.Simple(context.granteeName());
			var roleNames = context.roleName().Select(SqlParseUtil.Name.Simple).ToArray();

			foreach (var roleName in roleNames) {
				AddStatement(context, new RevokeRoleStatement(grantee, roleName));
			}

			return base.VisitRevokeRoleStatement(context);
		}

		// TODO:
		//public override SqlStatement VisitRevokePrivilegeStatement(PlSqlParser.RevokePrivilegeStatementContext context) {
		//	var granteeName = SqlParseUtil.Name.Simple(context.granteeName());
		//	var objectName = SqlParseUtil.Name.Object(context.objectName());
		//	var grantOption = context.GRANT() != null && context.OPTION() != null;

		//	var privs = Privilege.None;
		//	if (context.ALL() != null) {
		//		privs = SqlPrivileges.TableAll;
		//	} else {
		//		var privNames = context.privilegeName().Select(x => x.GetText());

		//		foreach (var privName in privNames) {
		//			try {
		//				var priv = SqlPrivileges.Resolver.ResolvePrivilege(privName);
		//				privs += priv;
		//			} catch (Exception) {
		//				throw new ParseCanceledException("Invalid privilege specified.");
		//			}
		//		}
		//	}

		//	AddStatement(new RevokeObjectPrivilegesStatement(granteeName, privs, grantOption, objectName, new string[0]));

		//	return base.VisitRevokePrivilegeStatement(context);
		//}
	}
}
