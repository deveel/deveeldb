using System;

using Deveel.Data.Sql.Schemas;

namespace Deveel.Data.Security {
	static class QueryExtensions {
		public static void CreateAdminUser(this IQuery context, string userName, string identification, string token) {
			try {
				context.Access().CreateUser(userName, identification, token);

				// This is the admin user so add to the 'secure access' table.
				context.Access().AddUserToRole(userName, SystemRoles.SecureAccessRole);

				context.Access().GrantOnSchema(context.Session.Database().Context.DefaultSchema(), userName, Privileges.SchemaAll, true);
				context.Access().GrantOnSchema(SystemSchema.Name, userName, Privileges.SchemaRead);
				context.Access().GrantOnSchema(InformationSchema.SchemaName, userName, Privileges.SchemaRead);
			} catch (DatabaseSystemException) {
				throw;
			} catch (Exception ex) {
				throw new DatabaseSystemException("Could not create the database administrator.", ex);
			}
		}

		public static void CreateAdminUser(this IQuery query, string userName, string password) {
			query.CreateAdminUser(userName, KnownUserIdentifications.ClearText, password);
		}
	}
}
