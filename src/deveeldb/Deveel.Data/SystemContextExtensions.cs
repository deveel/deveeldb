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
using System.Collections.Generic;

using Deveel.Data.Caching;
using Deveel.Data.Configuration;
using Deveel.Data.Services;
using Deveel.Data.Sql.Compile;
using Deveel.Data.Sql.Query;

namespace Deveel.Data {
	public static class SystemContextExtensions {
		public static IDatabaseContext CreateDatabaseContext(this ISystemContext context, string name) {
			var dbConfig = new Configuration.Configuration();
			dbConfig.SetValue("database.name", name);
			return context.CreateDatabaseContext(dbConfig);
		}

		#region Configurations

		public static bool ReadOnly(this ISystemContext context) {
			return context.Configuration.GetBoolean("system.readOnly", false);
		}

		public static bool IgnoreIdentifiersCase(this ISystemContext context) {
			return context.Configuration.GetBoolean("system.ignoreIdCase", true);
		}

		// TODO: remove this from here...
		public static string DefaultSchema(this ISystemContext context) {
			return context.Configuration.GetString("database.defaultSchema","APP");
		}

		public static bool AutoCommit(this ISystemContext context) {
			return context.Configuration.GetBoolean("system.autoCommit");
		}

		#endregion

	
		#region Features

		public static ISqlCompiler SqlCompiler(this ISystemContext context) {
			return context.ResolveService<ISqlCompiler>();
		}

		public static void UseSqlCompiler<TCompiler>(this ISystemContext context) where TCompiler : ISqlCompiler {
			context.UseSqlCompiler(typeof(TCompiler));
		}

		public static void UseSqlCompiler(this ISystemContext context, Type compilerType) {
			if (compilerType == null)
				throw new ArgumentNullException("compilerType");

			if (!typeof(ISqlCompiler).IsAssignableFrom(compilerType))
				throw new ArgumentException(String.Format("The type '{0}' is not a SQL Compiler.", compilerType));

			context.RegisterService(compilerType);
		}

		public static void UseSqlCompiler(this ISystemContext context, ISqlCompiler compiler) {
			context.RegisterInstance(compiler);
		}

		public static void UseDefaultSqlCompiler(this ISystemContext context) {
			context.UseSqlCompiler<SqlDefaultCompiler>();
		}

		public static IQueryPlanner QueryPlanner(this ISystemContext context) {
			return context.ResolveService<IQueryPlanner>();
		}

		public static void UseDefaultQueryPlanner(this ISystemContext context) {
			context.RegisterService<QueryPlanner>();
		}

		public static ITableCellCache TableCellCache(this ISystemContext context) {
			return context.ResolveService<ITableCellCache>();
		}

		public static void UseDefaultTableCellCache(this ISystemContext context) {
			context.RegisterService<TableCellCache>();
		}

		#endregion
	}
}
